"""
Provides a class to act a bit like an NMC
"""
class FakeAntenna:
  """
  antenna simulator
  """
  def __init__(self, site="CDSCC", dss=43):
    self.wsn = 0
    self.site = site
    self.dss = dss

  def send(self, command_str):
    self.command = command_str
  
  def recv(self, recval):
    return "COMPLETED"
