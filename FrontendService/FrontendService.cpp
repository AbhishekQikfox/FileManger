#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <mutex>
#include <filesystem>
#include <chrono>
#include <iomanip>
#include <thread>
#include <future>
#include <queue>
#include <condition_variable>
#include <atomic>

#include <nlohmann/json.hpp>

#include "httplib.h"
#include <windows.h>
#include <ctime>
#include <string>
#include <vector>
#include <random>
#include <sstream>
#include <iomanip>
#include <openssl/sha.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/buffer.h>

#include <random>
#include <sstream>
#include <iomanip>

// Global log file
std::ofstream logFile("frontend.log", std::ios::app);

// Backend service configuration
const std::string BACKEND_HOST = "127.0.0.1";
const int BACKEND_PORT = 8081;
const std::string BACKEND_ENDPOINT = "/store";

std::string base64Encode(const std::vector<char>& data) {
    BIO* bio, * b64;
    BUF_MEM* bufferPtr;
    b64 = BIO_new(BIO_f_base64());
    bio = BIO_new(BIO_s_mem());
    bio = BIO_push(b64, bio);
    BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL); // no newlines
    BIO_write(bio, data.data(), static_cast<int>(data.size()));
    BIO_flush(bio);
    BIO_get_mem_ptr(bio, &bufferPtr);
    std::string result(bufferPtr->data, bufferPtr->length);
    BIO_free_all(bio);
    return result;
}

// 64-bit random hex suffix
static std::string randomHex64() {
    static std::random_device rd;
    static std::mt19937_64 eng(rd());
    uint64_t v = eng();
    std::ostringstream ss;
    ss << std::hex << std::setw(16) << std::setfill('0') << v;
    return ss.str();
}

// CID = SHA256(concat(chunkHashes)) + "_" + randomHex64()
std::string generateCID(const std::vector<std::string>& chunkHashes) {
    // 1) build one big string
    std::string combined;
    combined.reserve(chunkHashes.size() * 64);
    for (auto& h : chunkHashes) combined += h;

    // 2) SHA256 it
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(combined.data()),
           combined.size(),
           digest);

    // 3) hex-encode the digest
    std::ostringstream hex;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        hex << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(digest[i]);
    }

    // 4) append suffix
    hex << "_" << randomHex64();
    return hex.str();
}

void WriteLog(const std::string& message) {
    if (logFile.is_open()) {
        time_t now = time(0);
        char* dt = ctime(&now);
        dt[strlen(dt) - 1] = 0; // Remove newline
        logFile << dt << " - " << message << std::endl;
        logFile.flush();
    }
}

namespace fs = std::filesystem;
using json = nlohmann::json;

SERVICE_STATUS serviceStatus;
SERVICE_STATUS_HANDLE serviceStatusHandle;
static std::atomic<bool> paused(false);
std::atomic<bool> running(false);
// Allow RunServer() to see the paused flag
extern std::atomic<bool> paused;

std::mutex serverMutex;
std::condition_variable serverCV;
httplib::Server* serverPtr = nullptr;

const size_t CHUNK_SIZE = 1024 * 1024; // 1MB

class ThreadPool {
public:
    ThreadPool(unsigned int threads) : stop(false) {
        for (unsigned int i = 0; i < threads; ++i) {
            workers.emplace_back([this] {
                for (;;) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(queue_mutex);
                        condition.wait(lock, [this] { return stop.load() || !tasks.empty(); });
                        if (stop.load() && tasks.empty()) return;
                        task = std::move(tasks.front());
                        tasks.pop();
                    }
                    task();
                }
            });
        }
    }

    ~ThreadPool() {
        stop.store(true);
        condition.notify_all();
        for (std::thread &worker : workers)
            worker.join();
    }

    std::future<std::string> enqueue(std::function<std::string()> task) {
        auto packaged_task = std::make_shared<std::packaged_task<std::string()>>(task);
        std::future<std::string> result = packaged_task->get_future();
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            if (stop.load())
                throw std::runtime_error("Enqueue on stopped ThreadPool");
            tasks.push([packaged_task]() { (*packaged_task)(); });
        }
        condition.notify_one();
        return result;
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    std::atomic<bool> stop;
};

std::string computeHash(const std::vector<char>& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256((const unsigned char*)data.data(), data.size(), hash);
    std::string hex;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        char buf[3];
        sprintf(buf, "%02x", hash[i]);
        hex += buf;
    }
    return hex;
}

// Replace SendToBackendIPC with HTTP client
bool SendToBackendHTTP(const std::string& jsonPayload) {
    WriteLog("Connecting to backend service at " + BACKEND_HOST + ":" + std::to_string(BACKEND_PORT));
    
    httplib::Client client(BACKEND_HOST, BACKEND_PORT);
    client.set_connection_timeout(10); // 10 seconds timeout
    client.set_read_timeout(60); // 60 seconds read timeout for large payloads
    
    // Set appropriate content type
    httplib::Headers headers = {
        {"Content-Type", "application/json"}
    };
    
    // Send POST request with JSON payload
    auto res = client.Post(BACKEND_ENDPOINT, headers, jsonPayload, "application/json");
    
    if (!res) {
        WriteLog("Failed to connect to backend service");
        return false;
    }
    
    if (res->status != 200) {
        WriteLog("Backend service returned error: " + std::to_string(res->status) + " - " + res->body);
        return false;
    }
    
    WriteLog("Successfully sent metadata to backend. Response: " + res->body);
    return true;
}

void RunServer() {
    httplib::Server server;

    server.Post("/upload", [](const httplib::Request& req, httplib::Response& res) {
        if (!req.has_file("file")) {
            res.status = 400;
            res.set_content("Missing 'file' in upload", "text/plain");
            WriteLog("Upload rejected: Missing 'file' field");
            return;
        }

        const auto& file = req.get_file_value("file");
        WriteLog("Received upload: " + file.filename + " (" + std::to_string(file.content.size()) + " bytes)");

        // For very small files, just process directly
        if (file.content.size() < 1024 * 1024) { // Less than 1MB
            std::vector<char> data(file.content.begin(), file.content.end());
            std::vector<std::string> chunkHashes;
            std::vector<std::string> encodedChunks;

            size_t offset = 0;
            size_t chunkCount = 0;

            while (offset < data.size()) {
                size_t len = std::min(CHUNK_SIZE, data.size() - offset);
                std::vector<char> chunk(data.begin() + offset, data.begin() + offset + len);

                std::string hash = computeHash(chunk);
                chunkHashes.push_back(hash);

                std::string encodedChunk = base64Encode(chunk);
                encodedChunks.push_back(encodedChunk);

                offset += len;
                chunkCount++;
            }

            WriteLog("File divided into " + std::to_string(chunkCount) + " chunks");
            std::string cid = generateCID(chunkHashes);
            WriteLog("Generated CID: " + cid);

            // Build JSON payload
            json payload;
            payload["cid"] = cid;
            payload["chunks"] = chunkHashes;
            payload["data"] = encodedChunks;

            std::string jsonStr = payload.dump();
            WriteLog("JSON payload size: " + std::to_string(jsonStr.size()) + " bytes");

            if (!SendToBackendHTTP(jsonStr)) {
                WriteLog("Failed to send to backend");
                res.status = 500;
                res.set_content("Failed to send to backend", "text/plain");
                return;
            }

            WriteLog("Successfully sent to backend. Returning CID to client.");
            res.status = 200;
            res.set_content(cid, "text/plain");
            return;
        }

        // For larger files, process in smaller chunks to avoid memory issues
        std::vector<std::string> chunkHashes;
        std::vector<std::string> encodedChunks;
        const size_t processSize = 1024 * 1024; // Process 1MB at a time

        for (size_t fileOffset = 0; fileOffset < file.content.size(); fileOffset += processSize) {
            size_t processLength = std::min(processSize, file.content.size() - fileOffset);
            std::vector<char> processBuffer(file.content.begin() + fileOffset,
                file.content.begin() + fileOffset + processLength);

            size_t offset = 0;
            while (offset < processBuffer.size()) {
                size_t len = std::min(CHUNK_SIZE, processBuffer.size() - offset);
                std::vector<char> chunk(processBuffer.begin() + offset,
                    processBuffer.begin() + offset + len);

                std::string hash = computeHash(chunk);
                chunkHashes.push_back(hash);

                std::string encodedChunk = base64Encode(chunk);
                encodedChunks.push_back(encodedChunk);

                offset += len;
            }
        }

        WriteLog("File divided into " + std::to_string(chunkHashes.size()) + " chunks");
        std::string cid = generateCID(chunkHashes);
        WriteLog("Generated CID: " + cid);

        // Build JSON payload
        json payload;
        payload["cid"] = cid;
        payload["chunks"] = chunkHashes;
        payload["data"] = encodedChunks;

        std::string jsonStr = payload.dump();
        WriteLog("JSON payload size: " + std::to_string(jsonStr.size()) + " bytes");

        if (!SendToBackendHTTP(jsonStr)) {
            WriteLog("Failed to send to backend");
            res.status = 500;
            res.set_content("Failed to send to backend", "text/plain");
            return;
        }

        WriteLog("Successfully sent to backend. Returning CID to client.");
        res.status = 200;
        res.set_content(cid, "text/plain");
    });

    {
        std::lock_guard<std::mutex> lock(serverMutex);
        serverPtr = &server;
        serverCV.notify_one();
    }

    WriteLog("Server started on port 8080");
    server.listen("0.0.0.0", 8080);
    WriteLog("Server stopped");
}

void WINAPI ServiceCtrlHandler(DWORD controlCode) {
    switch (controlCode) {
    case SERVICE_CONTROL_STOP:
        running = false;
        serviceStatus.dwCurrentState = SERVICE_STOP_PENDING;
        SetServiceStatus(serviceStatusHandle, &serviceStatus);
        {
            std::lock_guard<std::mutex> lock(serverMutex);
            if (serverPtr) serverPtr->stop();
        }
        break;
    case SERVICE_CONTROL_PAUSE:
        paused = true;
        serviceStatus.dwCurrentState = SERVICE_PAUSED;
        SetServiceStatus(serviceStatusHandle, &serviceStatus);
        break;
    case SERVICE_CONTROL_CONTINUE:
        paused = false;
        serviceStatus.dwCurrentState = SERVICE_RUNNING;
        SetServiceStatus(serviceStatusHandle, &serviceStatus);
        break;
    }
}

void WINAPI ServiceMain(DWORD argc, LPSTR* argv) {
    serviceStatusHandle = RegisterServiceCtrlHandler("Processsing", ServiceCtrlHandler);
    serviceStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    serviceStatus.dwCurrentState = SERVICE_START_PENDING;
    serviceStatus.dwControlsAccepted = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_PAUSE_CONTINUE;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);

    running = true;
    serviceStatus.dwCurrentState = SERVICE_RUNNING;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);

    std::thread serverThread(RunServer);
    {
        std::unique_lock<std::mutex> lock(serverMutex);
        serverCV.wait(lock, [] { return serverPtr != nullptr; });
    }
    while (running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (serverThread.joinable()) serverThread.join();

    serviceStatus.dwCurrentState = SERVICE_STOPPED;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);
}

int main() {
    // Open log file
    std::string logDir = "C:\\Users\\Training\\Desktop\\ProjectRoot\\FrontendService";
    std::string logPath = logDir + "\\frontend.log";
    logFile.open(logPath, std::ios::app);
    WriteLog("Frontend service starting");

    SERVICE_TABLE_ENTRY serviceTable[] = {
        {"Processsing", ServiceMain},
        {NULL, NULL}
    };

    if (!StartServiceCtrlDispatcher(serviceTable)) {
        DWORD error = GetLastError();
        // If called from command line, not as service
        if (error == ERROR_FAILED_SERVICE_CONTROLLER_CONNECT) {
            WriteLog("Running as standalone application (not as service)");
            running = true;
            RunServer();
            return 0;
        }
        WriteLog("Failed to start service. Error: " + std::to_string(error));
    }

    return 0;
}