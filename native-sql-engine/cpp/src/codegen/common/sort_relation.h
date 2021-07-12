/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <arrow/buffer.h>
#include <arrow/compute/api.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/common/relation_column.h"
#include "precompile/type_traits.h"

using sparkcolumnarplugin::codegen::arrowcompute::extra::ArrayItemIndexS;
using sparkcolumnarplugin::precompile::enable_if_number;
using sparkcolumnarplugin::precompile::enable_if_string_like;
using sparkcolumnarplugin::precompile::FixedSizeBinaryArray;
using sparkcolumnarplugin::precompile::StringArray;
using sparkcolumnarplugin::precompile::TypeTraits;

class SortRelation {
 public:
  SortRelation(
      arrow::compute::ExecContext* ctx,
      std::shared_ptr<LazyBatchIterator> lazy_in,
      const std::vector<std::shared_ptr<RelationColumn>>& sort_relation_key_list,
      const std::vector<std::shared_ptr<RelationColumn>>& sort_relation_payload_list)
      : ctx_(ctx) {
    lazy_in_ = lazy_in;
    sort_relation_key_list_ = sort_relation_key_list;
    sort_relation_payload_list_ = sort_relation_payload_list;
  }

  ~SortRelation() {}

  void Advance(int shift) {
    std::shared_ptr<arrow::RecordBatch> batch = lazy_in_->GetBatch(requested_batches);
    int64_t batch_length = batch->num_rows();
    int64_t batch_remaining = batch_length - offset_in_current_batch_;
    if (shift <= batch_remaining) {
      offset_in_current_batch_ = offset_in_current_batch_ + shift;
      return;
    }
    int64_t remaining = shift - batch_remaining;
    int32_t batch_i = requested_batches + 1;
    while (true) {
      std::shared_ptr<arrow::RecordBatch> b = lazy_in_->GetBatch(batch_i);
      int64_t current_batch_length = batch->num_rows();
      if (remaining < current_batch_length) {
        requested_batches = batch_i;
        offset_in_current_batch_ = shift;
        return;
      }
      remaining -= current_batch_length;
      batch_i++;
    }
  }

  ArrayItemIndexS GetItemIndexWithShift(int shift) {
    std::shared_ptr<arrow::RecordBatch> batch = lazy_in_->GetBatch(requested_batches);
    int64_t batch_length = batch->num_rows();
    int64_t batch_remaining = batch_length - offset_in_current_batch_;
    if (shift <= batch_remaining) {
      ArrayItemIndexS s(offset_in_current_batch_ + shift, requested_batches);
      return s;
    }
    int64_t remaining = shift - batch_remaining;
    int32_t batch_i = requested_batches + 1;
    while (true) {
      std::shared_ptr<arrow::RecordBatch> b = lazy_in_->GetBatch(batch_i);
      int64_t current_batch_length = batch->num_rows();
      if (remaining < current_batch_length) {
        ArrayItemIndexS s(remaining, batch_i);
      }
      remaining -= current_batch_length;
      batch_i++;
    }
  }

  bool Next() {
    if (!CheckRangeBound(1)) return false;
    Advance(1);
    offset_++;
    range_cache_ = -1;
    return true;
  }

  bool NextNewKey() {
    auto range = GetSameKeyRange();
    if (!CheckRangeBound(range)) return false;
    Advance(range);
    offset_ += range;
    range_cache_ = -1;
    return true;
  }

  int GetSameKeyRange() {
    if (range_cache_ != -1) return range_cache_;
    if (!CheckRangeBound(0)) return 0;
    int range = 0;
    bool is_same = true;
    while (is_same) {
      if (CheckRangeBound(range + 1)) {
        auto cur_idx = GetItemIndexWithShift(range);
        auto cur_idx_plus_one = GetItemIndexWithShift(range + 1);
        for (auto col : sort_relation_key_list_) {
          if (!(is_same = col->IsEqualTo(cur_idx.array_id, cur_idx.id,
                                         cur_idx_plus_one.array_id, cur_idx_plus_one.id)))
            break;
        }
      } else {
        is_same = false;
      }
      if (!is_same) break;
      range++;
    }
    range += 1;
    range_cache_ = range;
    return range;
  }

  bool CheckRangeBound(int shift) {
    std::shared_ptr<arrow::RecordBatch> batch = lazy_in_->GetBatch(requested_batches);
    if (batch == nullptr) {
      return false;
    }
    int64_t batch_length = batch->num_rows();
    int64_t batch_remaining = batch_length - offset_in_current_batch_;
    int64_t remaining = shift - batch_remaining;
    int32_t batch_i = requested_batches + 1;
    while (remaining > 0) {
      std::shared_ptr<arrow::RecordBatch> b = lazy_in_->GetBatch(batch_i);
      if (batch == nullptr) {
        return false;
      }
      int64_t current_batch_length = batch->num_rows();
      remaining -= current_batch_length;
      batch_i++;
    }
    return true;
  }

  template <typename T>
  arrow::Status GetColumn(int idx, std::shared_ptr<T>* out) {
    *out = std::dynamic_pointer_cast<T>(sort_relation_payload_list_[idx]);
    return arrow::Status::OK();
  }

 protected:
  arrow::compute::ExecContext* ctx_;
  std::shared_ptr<LazyBatchIterator> lazy_in_;
  uint64_t offset_ = 0;
  int64_t offset_in_current_batch_ = 0;
  int32_t requested_batches = 0;
  int range_cache_ = -1;
  std::vector<std::shared_ptr<RelationColumn>> sort_relation_key_list_;
  std::vector<std::shared_ptr<RelationColumn>> sort_relation_payload_list_;
};
