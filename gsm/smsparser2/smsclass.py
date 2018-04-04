class SmsInbox:
	def __init__(self,inbox_id,msg,sim_num,ts):
		self.inbox_id = inbox_id
		self.msg = msg
		self.sim_num = sim_num
		self.ts=ts     

class DataTable:
	def __init__(self,name,data):
		self.name = name
		self.data = data

