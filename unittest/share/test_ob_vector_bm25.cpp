/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include <cmath>
#include <random>
#include <chrono>
#include <iostream>
#include "share/vector_type/ob_vector_bm25.h"

using namespace oceanbase::common;

class TestObVectorBM25 : public ::testing::Test
{
protected:
  static constexpr double EPSILON = 1e-10;

  void SetUp() override
  {
    // Initialize arch detection
    init_arches();
  }

  // Helper to compare floating point values
  static bool nearly_equal(double a, double b, double epsilon = EPSILON)
  {
    return std::fabs(a - b) < epsilon;
  }
};

// Test scalar implementation correctness
TEST_F(TestObVectorBM25, ScalarBasic)
{
  const int64_t count = 4;
  int64_t doc_token_cnts[4] = {100, 200, 150, 50};
  uint64_t related_token_cnts[4] = {5, 10, 3, 8};
  double avg_doc_token_cnt = 125.0;
  double token_weight = 2.5;
  double results[4] = {0};

  bm25_batch_scalar(doc_token_cnts, related_token_cnts, avg_doc_token_cnt, token_weight, results, count);

  // Verify results manually
  for (int64_t i = 0; i < count; ++i) {
    const double tf = static_cast<double>(related_token_cnts[i]);
    const double norm_len = static_cast<double>(doc_token_cnts[i]) / avg_doc_token_cnt;
    const double denom = tf + BM25_K1 * (1.0 - BM25_B + BM25_B * norm_len);
    const double expected = token_weight * (tf / denom);
    EXPECT_TRUE(nearly_equal(results[i], expected))
        << "Mismatch at index " << i << ": expected " << expected << ", got " << results[i];
  }
}

// Test SIMD vs Scalar parity with small batch
TEST_F(TestObVectorBM25, SimdScalarParity_SmallBatch)
{
  const int64_t count = 8;
  int64_t doc_token_cnts[8] = {100, 200, 150, 50, 300, 75, 180, 90};
  uint64_t related_token_cnts[8] = {5, 10, 3, 8, 2, 15, 7, 4};
  double avg_doc_token_cnt = 140.0;
  double token_weight = 3.2;

  double results_scalar[8] = {0};
  double results_simd[8] = {0};

  // Compute using scalar
  bm25_batch_scalar(doc_token_cnts, related_token_cnts, avg_doc_token_cnt, token_weight, results_scalar, count);

  // Compute using dispatcher (will use SIMD if available)
  bm25_batch_dispatch(doc_token_cnts, related_token_cnts, avg_doc_token_cnt, token_weight, results_simd, count);

  // Verify results match
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_TRUE(nearly_equal(results_scalar[i], results_simd[i]))
        << "Mismatch at index " << i << ": scalar=" << results_scalar[i] << ", simd=" << results_simd[i];
  }
}

// Test SIMD vs Scalar parity with larger batch
TEST_F(TestObVectorBM25, SimdScalarParity_LargeBatch)
{
  const int64_t count = 1024;
  std::vector<int64_t> doc_token_cnts(count);
  std::vector<uint64_t> related_token_cnts(count);
  std::vector<double> results_scalar(count);
  std::vector<double> results_simd(count);

  // Initialize with random data
  std::mt19937 gen(42);  // Fixed seed for reproducibility
  std::uniform_int_distribution<int64_t> doc_dist(10, 500);
  std::uniform_int_distribution<uint64_t> token_dist(1, 50);

  for (int64_t i = 0; i < count; ++i) {
    doc_token_cnts[i] = doc_dist(gen);
    related_token_cnts[i] = token_dist(gen);
  }

  double avg_doc_token_cnt = 150.0;
  double token_weight = 2.8;

  // Compute using scalar
  bm25_batch_scalar(doc_token_cnts.data(), related_token_cnts.data(),
                    avg_doc_token_cnt, token_weight, results_scalar.data(), count);

  // Compute using dispatcher
  bm25_batch_dispatch(doc_token_cnts.data(), related_token_cnts.data(),
                      avg_doc_token_cnt, token_weight, results_simd.data(), count);

  // Verify results match
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_TRUE(nearly_equal(results_scalar[i], results_simd[i]))
        << "Mismatch at index " << i << ": scalar=" << results_scalar[i] << ", simd=" << results_simd[i];
  }
}

// Test with odd-sized batches (test remainder handling)
TEST_F(TestObVectorBM25, SimdScalarParity_OddSizes)
{
  double avg_doc_token_cnt = 100.0;
  double token_weight = 2.0;

  // Test various odd sizes that don't align with SIMD lane widths
  for (int64_t count : {1, 3, 5, 7, 9, 15, 17, 31, 33, 63, 65, 127, 129}) {
    std::vector<int64_t> doc_token_cnts(count);
    std::vector<uint64_t> related_token_cnts(count);
    std::vector<double> results_scalar(count);
    std::vector<double> results_simd(count);

    for (int64_t i = 0; i < count; ++i) {
      doc_token_cnts[i] = 50 + i * 3;
      related_token_cnts[i] = 1 + (i % 10);
    }

    bm25_batch_scalar(doc_token_cnts.data(), related_token_cnts.data(),
                      avg_doc_token_cnt, token_weight, results_scalar.data(), count);

    bm25_batch_dispatch(doc_token_cnts.data(), related_token_cnts.data(),
                        avg_doc_token_cnt, token_weight, results_simd.data(), count);

    for (int64_t i = 0; i < count; ++i) {
      EXPECT_TRUE(nearly_equal(results_scalar[i], results_simd[i]))
          << "Mismatch at count=" << count << ", index " << i
          << ": scalar=" << results_scalar[i] << ", simd=" << results_simd[i];
    }
  }
}

// Test bm25_batch_compute API
TEST_F(TestObVectorBM25, BatchComputeAPI)
{
  const int64_t count = 16;
  std::vector<int64_t> doc_token_cnts(count, 100);
  std::vector<uint64_t> related_token_cnts(count, 5);
  std::vector<double> results_simd(count);
  std::vector<double> results_no_simd(count);

  double avg_doc_token_cnt = 100.0;
  double token_weight = 2.0;

  // Test with SIMD enabled
  int ret = bm25_batch_compute(doc_token_cnts.data(), related_token_cnts.data(),
                               avg_doc_token_cnt, token_weight, results_simd.data(), count, true);
  EXPECT_EQ(OB_SUCCESS, ret);

  // Test with SIMD disabled
  ret = bm25_batch_compute(doc_token_cnts.data(), related_token_cnts.data(),
                           avg_doc_token_cnt, token_weight, results_no_simd.data(), count, false);
  EXPECT_EQ(OB_SUCCESS, ret);

  // Results should match
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_TRUE(nearly_equal(results_simd[i], results_no_simd[i]));
  }
}

// Test edge cases
TEST_F(TestObVectorBM25, EdgeCases)
{
  double avg_doc_token_cnt = 100.0;
  double token_weight = 2.0;

  // Empty batch
  {
    double result;
    int ret = bm25_batch_compute(nullptr, nullptr, avg_doc_token_cnt, token_weight, &result, 0, true);
    EXPECT_EQ(OB_SUCCESS, ret);
  }

  // Single element
  {
    int64_t doc_cnt = 100;
    uint64_t token_cnt = 5;
    double result_scalar, result_simd;

    bm25_batch_scalar(&doc_cnt, &token_cnt, avg_doc_token_cnt, token_weight, &result_scalar, 1);
    bm25_batch_dispatch(&doc_cnt, &token_cnt, avg_doc_token_cnt, token_weight, &result_simd, 1);

    EXPECT_TRUE(nearly_equal(result_scalar, result_simd));
  }

  // Very large values
  {
    int64_t doc_cnt = 1000000;
    uint64_t token_cnt = 10000;
    double result_scalar, result_simd;

    bm25_batch_scalar(&doc_cnt, &token_cnt, 500000.0, token_weight, &result_scalar, 1);
    bm25_batch_dispatch(&doc_cnt, &token_cnt, 500000.0, token_weight, &result_simd, 1);

    EXPECT_TRUE(nearly_equal(result_scalar, result_simd));
  }

  // Zero token frequency (edge case)
  {
    int64_t doc_cnt = 100;
    uint64_t token_cnt = 0;
    double result_scalar, result_simd;

    bm25_batch_scalar(&doc_cnt, &token_cnt, avg_doc_token_cnt, token_weight, &result_scalar, 1);
    bm25_batch_dispatch(&doc_cnt, &token_cnt, avg_doc_token_cnt, token_weight, &result_simd, 1);

    EXPECT_TRUE(nearly_equal(result_scalar, result_simd));
    EXPECT_TRUE(nearly_equal(result_scalar, 0.0));  // tf=0 should give score=0
  }
}

// Test invalid arguments
TEST_F(TestObVectorBM25, InvalidArguments)
{
  int64_t doc_cnt = 100;
  uint64_t token_cnt = 5;
  double result;

  // Null pointers
  EXPECT_EQ(OB_INVALID_ARGUMENT, bm25_batch_compute(nullptr, &token_cnt, 100.0, 2.0, &result, 1, true));
  EXPECT_EQ(OB_INVALID_ARGUMENT, bm25_batch_compute(&doc_cnt, nullptr, 100.0, 2.0, &result, 1, true));
  EXPECT_EQ(OB_INVALID_ARGUMENT, bm25_batch_compute(&doc_cnt, &token_cnt, 100.0, 2.0, nullptr, 1, true));

  // Invalid avg_doc_token_cnt
  EXPECT_EQ(OB_INVALID_ARGUMENT, bm25_batch_compute(&doc_cnt, &token_cnt, 0.0, 2.0, &result, 1, true));
  EXPECT_EQ(OB_INVALID_ARGUMENT, bm25_batch_compute(&doc_cnt, &token_cnt, -1.0, 2.0, &result, 1, true));

  // Negative count
  EXPECT_EQ(OB_INVALID_ARGUMENT, bm25_batch_compute(&doc_cnt, &token_cnt, 100.0, 2.0, &result, -1, true));
}

// Test to verify which SIMD path is being used
TEST_F(TestObVectorBM25, VerifySimdPath)
{
  std::cout << "\n========== SIMD Capability Check ==========" << std::endl;
  std::cout << "OB_USE_MULTITARGET_CODE: " << OB_USE_MULTITARGET_CODE << std::endl;

#if OB_USE_MULTITARGET_CODE
  std::cout << "AVX2 supported: " << (is_arch_supported(ObTargetArch::AVX2) ? "YES" : "NO") << std::endl;
  std::cout << "SSE42 supported: " << (is_arch_supported(ObTargetArch::SSE42) ? "YES" : "NO") << std::endl;
#endif

#if defined(__aarch64__)
  std::cout << "NEON supported: " << (is_arch_supported(ObTargetArch::NEON) ? "YES" : "NO") << std::endl;
#endif

  // Determine which path will be used
  std::cout << "\n>>> Expected SIMD path: ";
#if OB_USE_MULTITARGET_CODE
  if (is_arch_supported(ObTargetArch::AVX2)) {
    std::cout << "AVX2 (4 doubles/cycle)" << std::endl;
  } else if (is_arch_supported(ObTargetArch::SSE42)) {
    std::cout << "SSE4.2 (2 doubles/cycle)" << std::endl;
  } else {
    std::cout << "Scalar fallback" << std::endl;
  }
#elif defined(__aarch64__)
  if (is_arch_supported(ObTargetArch::NEON)) {
    std::cout << "NEON (2 doubles/cycle)" << std::endl;
  } else {
    std::cout << "Scalar fallback" << std::endl;
  }
#else
  std::cout << "Scalar fallback (SIMD not compiled)" << std::endl;
#endif

  std::cout << "============================================\n" << std::endl;

  // Quick benchmark to verify SIMD is working
  const int64_t count = 10000;
  std::vector<int64_t> doc_token_cnts(count, 100);
  std::vector<uint64_t> related_token_cnts(count, 5);
  std::vector<double> results(count);

  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 100; ++i) {
    bm25_batch_dispatch(doc_token_cnts.data(), related_token_cnts.data(),
                        100.0, 2.5, results.data(), count);
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

  std::cout << "Benchmark: 100 iterations x " << count << " elements = "
            << duration << " us (" << (100.0 * count / duration) << " M elements/sec)" << std::endl;
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

