#include <windows.h>
#include <winsvc.h>
#include <iostream>
#include <fstream>
#include <ctime>
#include <csignal>

bool running = false;
SERVICE_STATUS serviceStatus;
SERVICE_STATUS_HANDLE serviceStatusHandle;
std::ofstream logFile;

void WriteLog(const std::string& message) {
    if (logFile.is_open()) {
        time_t now = time(0);
        char* dt = ctime(&now);
        dt[strlen(dt) - 1] = 0; // Remove newline
        logFile << dt << " - " << message << std::endl;
        logFile.flush();
    }
}

void WINAPI ServiceCtrlHandler(DWORD controlCode) {
    switch (controlCode) {
        case SERVICE_CONTROL_STOP:
            running = false;
            serviceStatus.dwCurrentState = SERVICE_STOP_PENDING;
            SetServiceStatus(serviceStatusHandle, &serviceStatus);
            break;
        default:
            break;
    }
}

void WINAPI ServiceMain(DWORD argc, LPTSTR *argv) {
    serviceStatusHandle = RegisterServiceCtrlHandler("MyBasicService", ServiceCtrlHandler);
    if (!serviceStatusHandle) {
        return;
    }
    serviceStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    serviceStatus.dwCurrentState = SERVICE_START_PENDING;
    serviceStatus.dwControlsAccepted = SERVICE_ACCEPT_STOP;
    serviceStatus.dwWin32ExitCode = 0;
    serviceStatus.dwServiceSpecificExitCode = 0;
    serviceStatus.dwCheckPoint = 0;
    serviceStatus.dwWaitHint = 0;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);

    logFile.open("C:\\Windows\\Temp\\myservice.log", std::ios::app);
    if (!logFile) {
        serviceStatus.dwCurrentState = SERVICE_STOPPED;
        SetServiceStatus(serviceStatusHandle, &serviceStatus);
        return;
    }
    running = true;
    serviceStatus.dwCurrentState = SERVICE_RUNNING;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);

    while (running) {
        WriteLog("Service is running");
        Sleep(5000); // Sleep for 5 seconds
    }

    logFile.close();
    serviceStatus.dwCurrentState = SERVICE_STOPPED;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);
}

void SignalHandler(int signal) {
    if (signal == SIGINT) {
        running = false;
    }
}

int main() {
    SERVICE_TABLE_ENTRY serviceTable[] = {
        {"MyBasicService", ServiceMain},
        {NULL, NULL}
    };
    if (!StartServiceCtrlDispatcher(serviceTable)) {
        if (GetLastError() == ERROR_FAILED_SERVICE_CONTROLLER_CONNECT) {
            logFile.open("C:\\Windows\\Temp\\myservice.log", std::ios::app);
            if (!logFile) {
                std::cerr << "Failed to open log file" << std::endl;
                return 1;
            }
            running = true;
            signal(SIGINT, SignalHandler);
            while (running) {
                WriteLog("Service is running (console mode)");
                Sleep(5000);
            }
            logFile.close();
        } else {
            std::cerr << "Failed to start service" << std::endl;
            return 1;
        }
    }
    return 0;
}
