import os, time
import re
import configparser
from pprint import pprint
import db_lib as dbLib
import sys
from datetime import datetime as dt

class CentralServer:
	def __new__(self):
		config = configparser.ConfigParser()
		config.read('/home/pi/updews-pycodes/gsm/gsmserver_dewsl3/utils/config.cnf')
		config["CENTRAL_SERVER_NUMBER"]
		return config

class Parser:

	central_numbers = None
	gauge_names = ['INAG','INATB','MARG','NOAH_1795','PEPG','BLCSB']

	def __init__(self):
		self.db = dbLib.DatabaseConnection()
		self.central_numbers = CentralServer()
		print(">> Initialize Parser...")

	def parse_raw_data(self, raw_data):
		result = None
		try:
			table_reference = {
				"RiskAssessmentSummary":"risk_assessment_summary",
				"RiskAssessmentFamilyRiskProfile":"family_profile",
				"RiskAssessmentHazardData":"hazard_data",
				"RiskAssessmentRNC":"resources_and_capacities",
				"FieldSurveyLogs":"field_survey_logs",
				"SurficialDataCurrentMeasurement": "ground_measurement",
				"SurficialDataMomsSummary": "manifestations_of_movements",
				"MoMsReport": "manifestations_of_movements"
			}

			for raw in raw_data:
				rain_indicator = raw.data.split("|?=")
				if rain_indicator[0] in self.gauge_names and len(rain_indicator) == 8:
					result = self.db.write_central_raw_data(raw)
					if type(result) == str:
						result = self.parse_rain_data(rain_indicator)
						print("<< Inserting rain gauge data to DB.")
						print(">> Data ID: ", result)
					else:
						print(">> Error: Parsing raw data from central server.")
						return False
				else:
					is_logger = self.db.get_all_logger_mobile(raw.simnum)
					if len(is_logger) != 0:
						return False
					sender_detail = self.db.get_user_data(raw.simnum)
					if (len(sender_detail) != 0):
						sender = {
							"full_name": sender_detail[0][2]+" "+ sender_detail[0][3],
							"user_id": sender_detail[0][0],
							"account_id": sender_detail[0][1]
						}	
					deconstruct = raw.data.split(":")
					key = deconstruct[0]
					actual_raw_data = deconstruct[1].split("||")
					data = []
					for objData in actual_raw_data:
						data.append(objData.split("<*>"))
				
					if (key == "MoMsReport"):
						print(">> Initialize MoMs Reporting...")
						self.disseminateToExperts(data[0][0],data[0][2],data[0][1],data[0][3],sender)
					else:
						result = self.db.execute_syncing(table_reference[key], data)
						self.syncing_acknowledgement(key, result, sender)
			result = True
		except IndexError:
			print(">> Normal Message")
			result = False
		return result

	def syncing_acknowledgement(self, key, result, sender):
		print(">> Sending sync acknowledgement...")
		sim_num_container = []
		if (len(result) == 0):
			sim_nums = self.db.get_sync_acknowledgement_recipients()
			for sim_num in sim_nums:
				sim_num_container.append(sim_num[0])

			message = "CBEWS-L Sync Ack\n\nStatus: Synced\nModule: %s " \
				"\nTimestamp: %s\nSynced by: %s (ID: %s)" % (key, 
					dt.today().strftime("%A, %B %d, %Y, %X"), sender["full_name"], sender["account_id"])

			for number in sim_num_container:
				insert_smsoutbox = self.db.write_outbox(
					message=message, recipients=number, table='users')
			print(">> Acknowledgement sent...")
		else:
			print(">> Failed to sync data to server...")
	
	def disseminateToExperts(self, feature, feature_name, description, tos, sender):
		message = "Manifestation of Movement Report (UMI)\n\n" \
		"Time of observations: %s\n"\
		"Feature type: %s (%s)\nDescription: %s\n" % (tos, feature, feature_name, description)
		moms_status = self.sync_moms_data(feature, feature_name, description, tos)
		if moms_status != 0:
			for num in self.central_numbers:
				insert_smsoutbox = self.db.write_outbox( 
					message=message, recipients=num, table='users')
			print(">> Acknowledgement sent...")
		else:
			print(">> Failed to insert MoMs to server.. Rollback...")
	
	def sync_moms_data(self, feature, feature_name, description, tos):
		status = self.db.insert_MoMs_entry_via_sms(feature, feature_name, description, tos)
		return status
	
	def parse_rain_data(self, data):
		query = "INSERT INTO senslopedb.rain_%s VALUES (0,'%s',%s, " \
			"%s, %s, %s, %s, " \
			"%s)" % (data[0].lower(), data[1], data[2], data[3], data[4], data[5], data[6], data[7])
		id = self.db.write_to_db(query, True)
		return id
