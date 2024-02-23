import time
import servicemanager
import socket
import sys
import os
import win32serviceutil
import win32service
from email_sender import send_email
from project_secrets import ADMIN_EMAIL_RECIPIENTS, SCHEDULED_TIMES ,BUSINESS_NAME
from datetime import datetime
from replication import main

#py backgroundService.py install

class MyDaemonService(win32serviceutil.ServiceFramework):
    _svc_name_ = 'BoulderReplicationService'
    _svc_display_name_ = 'BoulderReplicationService'

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.is_alive = True

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.is_alive = False

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    def main(self):
        while self.is_alive:
            current_time = time.localtime()
            current_hour_minute = (current_time.tm_hour, current_time.tm_min)

            if current_hour_minute in SCHEDULED_TIMES:
                self.my_function()

            time.sleep(60)

    def my_function(self):
        # Call your function to generate reports
        main()

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(MyDaemonService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(MyDaemonService)
