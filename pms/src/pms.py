import pandas as pd
import pmsModel as model
import sys
import json
from StringIO import StringIO

def insert_accuracy_report(report):

	if len(report['submetrics']) == 0 or report['submetrics'] is None:
		status = model.insertAccuracy(report)
	else:
		status = model.insertAccuracyWithSubmetric(report)
	return status


def insert_timeliness_report(report):
	if len(report['submetrics']) == 0 or report['submetrics'] is None:
		status = model.insertTimeliness(report)
	else:
		status = model.insertTimelinessWithSubmetric(report)
	return status


def insert_error_log_report(report):
	if len(report['submetrics']) == 0 or report['submetrics'] is None:
		status = model.insertErrorLog(report)
	else:
		status = model.insertErrorLogWithSubmetric(report)
	return status