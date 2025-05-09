#include <benchmark/benchmark.h>
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <random>
#include <stdexcept>
#include <sqlite3.h>
#include <filesystem>
#include <iostream>
#include <chrono>

namespace fs = std::filesystem;

const std::string SERVICE_HOST = "127.0.0.1";
const int SERVICE_PORT = 8080;
const std::string TEST_FILENAME = "testfile.bin";
const std::string DB_PATH = "C:/Users/Training/Desktop/ProjectRoot/BackendService/chunks.db";
const std::string CHUNK_DIR = "C:/Users/Training/Desktop/ProjectRoot/BackendService/Chunks";
const std::vector<size_t> FILE_SIZES = {1024 * 1024, 5 * 1024 * 1024, 10 * 1024 * 1024}; // 1MB, 5MB, 10MB

std::string generate_file_data(size_t size) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255);
    std::string data(size, 0);
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<char>(dis(gen));
    }
    return data;
}

std::string setup_test_file(size_t file_size, const std::string& filename_suffix = "") {
    httplib::SSLClient cli(SERVICE_HOST, SERVICE_PORT);
    cli.enable_server_certificate_verification(false);

    std::string file_data = generate_file_data(file_size);
    httplib::MultipartFormDataItems items = {
        {"file", file_data, TEST_FILENAME + filename_suffix, "application/octet-stream"}
    };

    auto res = cli.Post("/upload", items);
    if (!res || res->status != 200) {
        throw std::runtime_error("Failed to setup test file: Status " + 
            (res ? std::to_string(res->status) + ", Body: " + res->body : "no response"));
    }
    return res->body;
}

void cleanup_benchmark_data() {
    std::cout << "Starting database cleanup...\n";

    sqlite3* db;
    if (sqlite3_open(DB_PATH.c_str(), &db) != SQLITE_OK) {
        std::cerr << "Failed to open database: " << sqlite3_errmsg(db) << "\n";
        sqlite3_close(db);
        return;
    }

    const char* queries[] = {
        "DELETE FROM file_chunks WHERE cid LIKE 'benchmark_test_cid%';",
        "DELETE FROM file_metadata WHERE cid LIKE 'benchmark_test_cid%';",
        "DELETE FROM chunk_references WHERE hash IN (SELECT hash FROM chunk_references WHERE EXISTS (SELECT 1 FROM file_chunks WHERE file_chunks.hash = chunk_references.hash AND file_chunks.cid LIKE 'benchmark_test_cid%'));",
        "DELETE FROM chunk_references WHERE NOT EXISTS (SELECT 1 FROM file_chunks WHERE file_chunks.hash = chunk_references.hash);"
    };

    for (const char* query : queries) {
        char* err_msg = nullptr;
        if (sqlite3_exec(db, query, nullptr, nullptr, &err_msg) != SQLITE_OK) {
            std::cerr << "Failed to execute query: " << err_msg << "\n";
            sqlite3_free(err_msg);
        } else {
            std::cout << "Executed: " << query << "\n";
        }
    }

    sqlite3_close(db);

    try {
        for (const auto& entry : fs::directory_iterator(CHUNK_DIR)) {
            if (entry.path().filename().string().find("chunk_") == 0) {
                fs::remove(entry.path());
                std::cout << "Deleted: " << entry.path() << "\n";
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Failed to clean chunk files: " << e.what() << "\n";
    }
}

static void BM_Upload(benchmark::State& state) {
    httplib::SSLClient cli(SERVICE_HOST, SERVICE_PORT);
    cli.enable_server_certificate_verification(false);
    size_t file_size = FILE_SIZES[state.range(0)];
    std::string file_data = generate_file_data(file_size);
    static int counter = 0;
    double total_time = 0.0;

    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();
        httplib::MultipartFormDataItems items = {
            {"file", file_data, TEST_FILENAME + std::to_string(counter++), "application/octet-stream"}
        };
        auto res = cli.Post("/upload", items);
        auto end = std::chrono::high_resolution_clock::now();
        total_time += std::chrono::duration<double, std::milli>(end - start).count();

        std::string cid = res && res->status == 200 ? res->body : "N/A";
        if (res) {
            double avg_time = total_time / state.iterations();
            state.SetLabel("Status: " + std::to_string(res->status) + ", CID: " + cid +
                ", Avg Time: " + std::to_string(avg_time) + " ms, Total Time: " + std::to_string(total_time) + " ms");
        }
    }
}

static void BM_GetFile(benchmark::State& state) {
    httplib::SSLClient cli(SERVICE_HOST, SERVICE_PORT);
    cli.enable_server_certificate_verification(false);
    size_t file_size = FILE_SIZES[state.range(0)];
    std::string cid = setup_test_file(file_size);
    double total_time = 0.0;

    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();
        auto res = cli.Get("/files/" + cid);
        auto end = std::chrono::high_resolution_clock::now();
        total_time += std::chrono::duration<double, std::milli>(end - start).count();

        if (res) {
            double avg_time = total_time / state.iterations();
            state.SetLabel("Status: " + std::to_string(res->status) + ", CID: " + cid +
                ", Avg Time: " + std::to_string(avg_time) + " ms, Total Time: " + std::to_string(total_time) + " ms");
        }
    }
}

static void BM_Update(benchmark::State& state) {
    httplib::SSLClient cli(SERVICE_HOST, SERVICE_PORT);
    cli.enable_server_certificate_verification(false);
    size_t file_size = FILE_SIZES[state.range(0)];
    std::string current_cid = setup_test_file(file_size, "_initial");
    double total_time = 0.0;

    for (auto _ : state) {
        std::string new_file_data = generate_file_data(file_size);
        httplib::MultipartFormDataItems items = {
            {"file", new_file_data, TEST_FILENAME, "application/octet-stream"}
        };
        auto start = std::chrono::high_resolution_clock::now();
        auto res = cli.Put("/update/" + current_cid, items);
        auto end = std::chrono::high_resolution_clock::now();
        total_time += std::chrono::duration<double, std::milli>(end - start).count();

        if (res && res->status == 200) {
            double avg_time = total_time / state.iterations();
            state.SetLabel("Status: 200, Old CID: " + current_cid + ", New CID: " + res->body +
                ", Avg Time: " + std::to_string(avg_time) + " ms, Total Time: " + std::to_string(total_time) + " ms");
            current_cid = res->body;
        }
    }
}

static void BM_Delete(benchmark::State& state) {
    httplib::SSLClient cli(SERVICE_HOST, SERVICE_PORT);
    cli.enable_server_certificate_verification(false);
    size_t file_size = FILE_SIZES[state.range(0)];
    double total_time = 0.0;

    for (auto _ : state) {
        std::string cid = setup_test_file(file_size, "_" + std::to_string(state.iterations()));
        auto start = std::chrono::high_resolution_clock::now();
        auto res = cli.Delete("/files/" + cid);
        auto end = std::chrono::high_resolution_clock::now();
        total_time += std::chrono::duration<double, std::milli>(end - start).count();

        if (res) {
            double avg_time = total_time / state.iterations();
            state.SetLabel("Status: " + std::to_string(res->status) + ", CID: " + cid +
                ", Avg Time: " + std::to_string(avg_time) + " ms, Total Time: " + std::to_string(total_time) + " ms");
        }
    }
}

BENCHMARK(BM_Upload)->Args({0})->Args({1})->Args({2})->Iterations(50)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_GetFile)->Args({0})->Args({1})->Args({2})->Iterations(50)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_Update)->Args({0})->Args({1})->Args({2})->Iterations(50)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_Delete)->Args({0})->Args({1})->Args({2})->Iterations(50)->Unit(benchmark::kMillisecond);

int main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    cleanup_benchmark_data();
    return 0;
}