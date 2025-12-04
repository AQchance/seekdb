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

#ifndef OCEANBASE_SHARE_VECTOR_TYPE_OB_VECTOR_BM25_H_
#define OCEANBASE_SHARE_VECTOR_TYPE_OB_VECTOR_BM25_H_

#include "common/ob_target_specific.h"
#include "lib/utility/ob_macro_utils.h"

#if OB_USE_MULTITARGET_CODE
#include <immintrin.h>
#endif

#if defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace oceanbase
{
namespace common
{

/**
 * SIMD-accelerated BM25 relevance calculation kernels.
 *
 * BM25 formula:
 *   doc_weight = tf / (tf + k1 * (1.0 - b + b * norm_len))
 *   relevance = token_weight * doc_weight
 *
 * where:
 *   tf = related_token_cnt (token frequency in document)
 *   norm_len = doc_token_cnt / avg_doc_token_cnt
 *   k1 = 1.2, b = 0.75 (BM25 parameters)
 *   token_weight is pre-computed from IDF
 */

// BM25 parameters (matching ob_expr_bm25.h)
static constexpr double BM25_K1 = 1.2;
static constexpr double BM25_B = 0.75;

/**
 * Scalar reference implementation for BM25 batch computation.
 * Used as fallback and for correctness verification.
 */
inline static void bm25_batch_scalar(
    const int64_t *doc_token_cnts,
    const uint64_t *related_token_cnts,
    const double avg_doc_token_cnt,
    const double token_weight,
    double *results,
    const int64_t count)
{
  const double inv_avg = 1.0 / avg_doc_token_cnt;
  const double k1_times_b = BM25_K1 * BM25_B;
  const double k1_times_one_minus_b = BM25_K1 * (1.0 - BM25_B);

  for (int64_t i = 0; i < count; ++i) {
    const double tf = static_cast<double>(related_token_cnts[i]);
    const double norm_len = static_cast<double>(doc_token_cnts[i]) * inv_avg;
    const double denom = tf + k1_times_one_minus_b + k1_times_b * norm_len;
    const double doc_weight = tf / denom;
    results[i] = token_weight * doc_weight;
  }
}

// Default implementation (fallback)
OB_DECLARE_DEFAULT_CODE(
  inline static void bm25_batch(
      const int64_t *doc_token_cnts,
      const uint64_t *related_token_cnts,
      const double avg_doc_token_cnt,
      const double token_weight,
      double *results,
      const int64_t count)
  {
    bm25_batch_scalar(doc_token_cnts, related_token_cnts, avg_doc_token_cnt,
                      token_weight, results, count);
  }
)

#if OB_USE_MULTITARGET_CODE

// SSE4.2 implementation: process 2 doubles at a time
OB_DECLARE_SSE42_SPECIFIC_CODE(
  inline static void bm25_batch(
      const int64_t *doc_token_cnts,
      const uint64_t *related_token_cnts,
      const double avg_doc_token_cnt,
      const double token_weight,
      double *results,
      const int64_t count)
  {
    const double inv_avg = 1.0 / avg_doc_token_cnt;
    const double k1_times_b = BM25_K1 * BM25_B;
    const double k1_times_one_minus_b = BM25_K1 * (1.0 - BM25_B);

    // Broadcast constants to 128-bit registers (2 doubles)
    const __m128d v_inv_avg = _mm_set1_pd(inv_avg);
    const __m128d v_k1_b = _mm_set1_pd(k1_times_b);
    const __m128d v_k1_1mb = _mm_set1_pd(k1_times_one_minus_b);
    const __m128d v_token_weight = _mm_set1_pd(token_weight);

    int64_t i = 0;
    // Process 2 elements at a time
    for (; i + 2 <= count; i += 2) {
      // Manually convert int64 to double (no native instruction)
      __m128d v_doc_cnt = _mm_set_pd(
          static_cast<double>(doc_token_cnts[i + 1]),
          static_cast<double>(doc_token_cnts[i]));
      __m128d v_tf = _mm_set_pd(
          static_cast<double>(related_token_cnts[i + 1]),
          static_cast<double>(related_token_cnts[i]));

      // norm_len = doc_token_cnt * inv_avg
      __m128d v_norm_len = _mm_mul_pd(v_doc_cnt, v_inv_avg);

      // denom = tf + k1*(1-b) + k1*b*norm_len
      __m128d v_denom = _mm_add_pd(v_k1_1mb, _mm_mul_pd(v_k1_b, v_norm_len));
      v_denom = _mm_add_pd(v_denom, v_tf);

      // doc_weight = tf / denom
      __m128d v_doc_weight = _mm_div_pd(v_tf, v_denom);

      // relevance = token_weight * doc_weight
      __m128d v_relevance = _mm_mul_pd(v_token_weight, v_doc_weight);

      // Store results
      _mm_storeu_pd(results + i, v_relevance);
    }

    // Handle remaining element
    for (; i < count; ++i) {
      const double tf = static_cast<double>(related_token_cnts[i]);
      const double norm_len = static_cast<double>(doc_token_cnts[i]) * inv_avg;
      const double denom = tf + k1_times_one_minus_b + k1_times_b * norm_len;
      const double doc_weight = tf / denom;
      results[i] = token_weight * doc_weight;
    }
  }
)

// AVX2 implementation: process 4 doubles at a time
OB_DECLARE_AVX2_SPECIFIC_CODE(
  inline static void bm25_batch(
      const int64_t *doc_token_cnts,
      const uint64_t *related_token_cnts,
      const double avg_doc_token_cnt,
      const double token_weight,
      double *results,
      const int64_t count)
  {
    const double inv_avg = 1.0 / avg_doc_token_cnt;
    const double k1_times_b = BM25_K1 * BM25_B;
    const double k1_times_one_minus_b = BM25_K1 * (1.0 - BM25_B);

    // Broadcast constants to 256-bit registers (4 doubles)
    const __m256d v_inv_avg = _mm256_set1_pd(inv_avg);
    const __m256d v_k1_b = _mm256_set1_pd(k1_times_b);
    const __m256d v_k1_1mb = _mm256_set1_pd(k1_times_one_minus_b);
    const __m256d v_token_weight = _mm256_set1_pd(token_weight);

    int64_t i = 0;
    // Process 4 elements at a time
    for (; i + 4 <= count; i += 4) {
      // Manually convert int64 to double (AVX2 doesn't have native int64->double)
      __m256d v_doc_cnt = _mm256_set_pd(
          static_cast<double>(doc_token_cnts[i + 3]),
          static_cast<double>(doc_token_cnts[i + 2]),
          static_cast<double>(doc_token_cnts[i + 1]),
          static_cast<double>(doc_token_cnts[i]));
      __m256d v_tf = _mm256_set_pd(
          static_cast<double>(related_token_cnts[i + 3]),
          static_cast<double>(related_token_cnts[i + 2]),
          static_cast<double>(related_token_cnts[i + 1]),
          static_cast<double>(related_token_cnts[i]));

      // norm_len = doc_token_cnt * inv_avg
      __m256d v_norm_len = _mm256_mul_pd(v_doc_cnt, v_inv_avg);

      // denom = tf + k1*(1-b) + k1*b*norm_len
      // Use FMA: k1_b * norm_len + k1_1mb
      __m256d v_denom = _mm256_fmadd_pd(v_k1_b, v_norm_len, v_k1_1mb);
      v_denom = _mm256_add_pd(v_denom, v_tf);

      // doc_weight = tf / denom
      __m256d v_doc_weight = _mm256_div_pd(v_tf, v_denom);

      // relevance = token_weight * doc_weight
      __m256d v_relevance = _mm256_mul_pd(v_token_weight, v_doc_weight);

      // Store results
      _mm256_storeu_pd(results + i, v_relevance);
    }

    // Handle remaining elements with scalar code
    for (; i < count; ++i) {
      const double tf = static_cast<double>(related_token_cnts[i]);
      const double norm_len = static_cast<double>(doc_token_cnts[i]) * inv_avg;
      const double denom = tf + k1_times_one_minus_b + k1_times_b * norm_len;
      const double doc_weight = tf / denom;
      results[i] = token_weight * doc_weight;
    }
  }
)

#endif // OB_USE_MULTITARGET_CODE

#if defined(__aarch64__)
// NEON implementation for ARM64: process 2 doubles at a time
inline static void bm25_batch_neon(
    const int64_t *doc_token_cnts,
    const uint64_t *related_token_cnts,
    const double avg_doc_token_cnt,
    const double token_weight,
    double *results,
    const int64_t count)
{
  const double inv_avg = 1.0 / avg_doc_token_cnt;
  const double k1_times_b = BM25_K1 * BM25_B;
  const double k1_times_one_minus_b = BM25_K1 * (1.0 - BM25_B);

  // Broadcast constants to NEON registers (2 doubles)
  const float64x2_t v_inv_avg = vdupq_n_f64(inv_avg);
  const float64x2_t v_k1_b = vdupq_n_f64(k1_times_b);
  const float64x2_t v_k1_1mb = vdupq_n_f64(k1_times_one_minus_b);
  const float64x2_t v_token_weight = vdupq_n_f64(token_weight);

  int64_t i = 0;
  // Process 2 elements at a time
  for (; i + 2 <= count; i += 2) {
    // Load and convert to double
    int64x2_t v_doc_cnt_i = vld1q_s64(doc_token_cnts + i);
    float64x2_t v_doc_cnt = vcvtq_f64_s64(v_doc_cnt_i);

    int64x2_t v_tf_i = vreinterpretq_s64_u64(vld1q_u64(related_token_cnts + i));
    float64x2_t v_tf = vcvtq_f64_s64(v_tf_i);

    // norm_len = doc_token_cnt * inv_avg
    float64x2_t v_norm_len = vmulq_f64(v_doc_cnt, v_inv_avg);

    // denom = tf + k1*(1-b) + k1*b*norm_len
    float64x2_t v_denom = vfmaq_f64(v_k1_1mb, v_k1_b, v_norm_len);
    v_denom = vaddq_f64(v_denom, v_tf);

    // doc_weight = tf / denom
    float64x2_t v_doc_weight = vdivq_f64(v_tf, v_denom);

    // relevance = token_weight * doc_weight
    float64x2_t v_relevance = vmulq_f64(v_token_weight, v_doc_weight);

    // Store results
    vst1q_f64(results + i, v_relevance);
  }

  // Handle remaining element
  for (; i < count; ++i) {
    const double tf = static_cast<double>(related_token_cnts[i]);
    const double norm_len = static_cast<double>(doc_token_cnts[i]) * inv_avg;
    const double denom = tf + k1_times_one_minus_b + k1_times_b * norm_len;
    const double doc_weight = tf / denom;
    results[i] = token_weight * doc_weight;
  }
}
#endif // __aarch64__

/**
 * Runtime dispatcher for BM25 batch computation.
 * Selects the best available SIMD implementation based on CPU capabilities.
 */
inline static void bm25_batch_dispatch(
    const int64_t *doc_token_cnts,
    const uint64_t *related_token_cnts,
    const double avg_doc_token_cnt,
    const double token_weight,
    double *results,
    const int64_t count)
{
#if OB_USE_MULTITARGET_CODE
  if (is_arch_supported(ObTargetArch::AVX2)) {
    specific::avx2::bm25_batch(doc_token_cnts, related_token_cnts,
                               avg_doc_token_cnt, token_weight, results, count);
    return;
  }
  if (is_arch_supported(ObTargetArch::SSE42)) {
    specific::sse42::bm25_batch(doc_token_cnts, related_token_cnts,
                                avg_doc_token_cnt, token_weight, results, count);
    return;
  }
#endif

#if defined(__aarch64__)
  if (is_arch_supported(ObTargetArch::NEON)) {
    bm25_batch_neon(doc_token_cnts, related_token_cnts,
                    avg_doc_token_cnt, token_weight, results, count);
    return;
  }
#endif

  // Scalar fallback
  bm25_batch_scalar(doc_token_cnts, related_token_cnts,
                    avg_doc_token_cnt, token_weight, results, count);
}

/**
 * Compute BM25 relevance scores for a batch of documents.
 * This is the main entry point for SIMD-accelerated BM25 computation.
 *
 * @param doc_token_cnts     Array of document token counts
 * @param related_token_cnts Array of related token counts (term frequency)
 * @param avg_doc_token_cnt  Average document length (must be > 0)
 * @param token_weight       Pre-computed query term weight (IDF component)
 * @param results            Output array for relevance scores (must be pre-allocated)
 * @param count              Number of elements to process
 * @param use_simd           Whether to use SIMD acceleration (default: true)
 * @return OB_SUCCESS on success
 */
inline int bm25_batch_compute(
    const int64_t *doc_token_cnts,
    const uint64_t *related_token_cnts,
    const double avg_doc_token_cnt,
    const double token_weight,
    double *results,
    const int64_t count,
    const bool use_simd = true)
{
  int ret = OB_SUCCESS;
  if (count == 0) {
    // Nothing to do, allow nullptr for empty batch
  } else if (OB_UNLIKELY(nullptr == doc_token_cnts || nullptr == related_token_cnts ||
                  nullptr == results || count < 0 || avg_doc_token_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (use_simd) {
    bm25_batch_dispatch(doc_token_cnts, related_token_cnts,
                        avg_doc_token_cnt, token_weight, results, count);
  } else {
    bm25_batch_scalar(doc_token_cnts, related_token_cnts,
                      avg_doc_token_cnt, token_weight, results, count);
  }
  return ret;
}

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_TYPE_OB_VECTOR_BM25_H_
