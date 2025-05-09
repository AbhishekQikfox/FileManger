#include <fstream>
#include <cstdio>
#include <filesystem>
#include <gtest/gtest.h>
#include "backend_utils.h"
#include <sqlite3.h>

// a helper: create a temp SQLite DB, create the table, insert known rows
static std::string makeTempDB() {
    std::string tmp = std::filesystem::temp_directory_path().string() + "/test.db";
    std::remove(tmp.c_str());
    sqlite3* db = nullptr;
    sqlite3_open(tmp.c_str(), &db);
    const char* sql = R"(
      CREATE TABLE file_chunks(cid TEXT, chunk_index INTEGER, hash TEXT);
      INSERT INTO file_chunks VALUES('CID1', 0, 'hashA');
      INSERT INTO file_chunks VALUES('CID1', 1, 'hashB');
    )";
    sqlite3_exec(db, sql, nullptr, nullptr, nullptr);
    sqlite3_close(db);
    return tmp;
}

TEST(BackendUtils, GetChunkHashesByCID) {
    auto db = makeTempDB();
    auto v = GetChunkHashesByCID("CID1", db);
    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0].first, 0);
    EXPECT_EQ(v[0].second, "hashA");
    EXPECT_EQ(v[1].first, 1);
    EXPECT_EQ(v[1].second, "hashB");
}

TEST(BackendUtils, ReadFileToVectorAndSHA) {
    // write a temp file
    auto tmpf = std::filesystem::temp_directory_path().string() + "/foo.bin";
    {
      std::ofstream o(tmpf, std::ios::binary);
      o << "abc";
    }
    auto data = ReadFileToVector(tmpf);
    EXPECT_EQ(data.size(), 3);
    EXPECT_EQ(computeSHA256(data),
              "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    std::remove(tmpf.c_str());
}

TEST(BackendUtils, TrimFunction) {
    EXPECT_EQ(trim("   abc  "), "abc");
    EXPECT_EQ(trim("\t test \n"), "test");
    EXPECT_EQ(trim("    "), "");
}

TEST(BackendUtils, GetChunkHashesByCIDEmpty) {
    auto db = makeTempDB();
    // CID not present in table
    auto v = GetChunkHashesByCID("UNKNOWN", db);
    EXPECT_TRUE(v.empty());
}

TEST(BackendUtils, ReadFileToVectorNonExistent) {
    auto data = ReadFileToVector("this_file_does_not_exist.bin");
    EXPECT_TRUE(data.empty());
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
