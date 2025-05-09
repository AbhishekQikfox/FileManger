#include <gtest/gtest.h>
#include "frontend_utils.h"
#include <openssl/sha.h>
#include <cctype>

// Helper: direct sha256 of std::string for validating prefixes
static std::string sha256(const std::string& s) {
    unsigned char buf[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(s.data()),
           s.size(), buf);
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i)
        oss << std::setw(2) << static_cast<int>(buf[i]);
    return oss.str();
}

TEST(StringToHex, Basic) {
    EXPECT_EQ(stringToHex("ABC"), "414243");
    EXPECT_EQ(stringToHex(std::string("\xff", 1)), "ff");
}

TEST(Trim, Various) {
    EXPECT_EQ(trim("   hello "), "hello");
    EXPECT_EQ(trim("\n\tX\r\n"), "X");
    EXPECT_EQ(trim(""), "");
}

TEST(ComputeHash, KnownValue) {
    std::vector<char> data{'a','b','c'};
    EXPECT_EQ(computeHash(data),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

TEST(GenerateCID, FormatAndPrefix) {
    std::vector<std::string> chunks = {
        sha256("one"),
        sha256("two")
    };
    auto cid = generateCID(chunks);
    // Must contain underscore separating prefix/suffix
    auto pos = cid.find('_');
    ASSERT_NE(pos, std::string::npos);
    // Prefix is sha256(concat hashes)
    std::string concat = chunks[0] + chunks[1];
    EXPECT_EQ(cid.substr(0, pos), sha256(concat));
    // Suffix is 16 hex chars
    std::string suffix = cid.substr(pos + 1);
    EXPECT_EQ(suffix.size(), 16u);
    for (char c : suffix) EXPECT_TRUE(std::isxdigit(static_cast<unsigned char>(c)));
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
