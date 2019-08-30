import os, time
import re
import configparser
from pprint import pprint
import db_lib as dbLib
import sys
from datetime import datetime as dt

class Parser:
	def __init__(self):
		self.db = dbLib.DatabaseConnection()
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
				
				print(table_reference[key])
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
		ct_phone = ['639175048863']
		message = "Manifestation of Movement Report (UMI)\n\n" \
		"Time of observations: %s\n"\
		"Feature type: %s (%s)\nDescription: %s\n" % (tos, feature, feature_name, description)
		moms_status = self.sync_moms_data(feature, feature_name, description, tos)
		if moms_status != 0:
			for num in ct_phone:
				insert_smsoutbox = self.db.write_outbox( 
					message=message, recipients=num, table='users')
			print(">> Acknowledgement sent...")
		else:
			print(">> Failed to insert MoMs to server.. Rollback...")
	
	def sync_moms_data(self, feature, feature_name, description, tos):
		status = self.db.insert_MoMs_entry_via_sms(feature, feature_name, description, tos)
		return status