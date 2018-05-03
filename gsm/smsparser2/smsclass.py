class SmsInbox:
	def __init__(self,inbox_id,msg,sim_num,ts):
		"""
		- The constructor for sms info.

		:param inbox_id: Inbox id number.
		:param msg: Sms message text.
		:param sim_num: Mobile number.
		:param ts: Message timestamp.
		:type inbox_id: int
		:type msg: str
		:type sim_num: in
		:type ts: date

		"""    
		self.inbox_id = inbox_id
		self.msg = msg
		self.sim_num = sim_num
		self.ts = ts     
class DataTable:
	def __init__(self,name,data):
		"""
		- The constructor for table data.

		:param name: Table name.
		:param data: Any type of data
		:type name: str
		:type data: any

		"""   
		self.name = name
		self.data = data


