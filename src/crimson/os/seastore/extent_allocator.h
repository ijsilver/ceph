// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/random_block_manager.h"

namespace crimson::os::seastore {

using extent_id_t = uint32_t;
using extent_off_t = uint64_t;

using ExtentRef = boost::intrusive_ptr<Extent>;

class ExtentAllocator {
public:
  using access_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::permission_denied,
    crimson::ct_error::enoent>;

  using mount_ertr = access_ertr;
  using mount_ret = access_ertr::future<>;
  mount_ret mount() {
    if (is_smr) {
      return segment_manager.mount();
    }
    else {
      return crimson::ct_error::input_output_error::make();
    }
  }
  mount_ret mount(const std::string &path, blk_paddr_t start) {
    if (is_smr) {
      return crimson::ct_error::input_output_error::make();
    }
    else {
      random_block_manager.open(path, start);
    }
  }

  using mkfs_ertr = access_ertr;
  using mkfs_ret = mkfs_ertr::future<>;
  mkfs_ret mkfs(seastore_meta_t meta) {
    if (is_smr) {
      return segment_manager.mkfs();
    }
    else {
      return crimson::ct_error::input_output_error::make();
    }
  }

  mkfs_ret mkfs(mkfs_config_t config) {
    if (is_smr) {
      return crimson::ct_error::input_output_error::make();
    }
    else {
      return random_block_manager.mkfs(config);
    }
  }

  using allocate_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent>;
  allocate_ertr::future<SegmentRef> allocate(extent_id_t id) {
    if (is_smr) {
      return segment_manager.open(id);
    }
    else {
      return crimson::ct_error::input_output_error::make();
    }
  }

  allocate_ertr::future<> allocate(
    Transaction &transaction,
    extent_off_t size) {
    if (is_smr) {
      return crimson::ct_error::input_output_error::make();
    }
    else {
      return random_block_manager.alloc_extent(transaction, size);
    }
  }

  using release_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent>;
  release_ertr::future<> release(extent_id_t id) {
    if (is_smr) {
      return segment_manager.release(id);
    }
    else {
      return crimson::ct_error::input_output_error::make();
    }
  }

  release_ertr::future<> release(
    Transaction &transaction,
    extent_off_t start,
    extent_off_t len) {
    if (is_smr) {
      return crimson::ct_error::input_output_error::make();
    }
    else {
      return random_block_manager.free_extent(transaction, start, len);
    }
  }

  using complete_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange
    >;
  complete_ertr::future<> complete(Transaction &transaction) {
    if (is_smr) {
      return crimson::ct_error::input_output_error::make();
    }
    else {
      return random_block_manager.complete_allocation(transaction);
    }
  }

  using read_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  read_ertr::future<> read(
    extent_off_t addr,
    extent_off_t len,
    ceph::bufferptr &out) {
    if (is_smr) {
      return segment_manager.read(addr, len, out);
    }
    else {
      return random_block_manager.read(addr, out);
    }
  }

  read_ertr::future<ceph::bufferptr> read(extent_off_t addr, extent_off_t len) {
    auto ptrref = std::make_unique<ceph::bufferptr>(
      buffer::create_page_aligned(len));
    return read(addr, len, *ptrref).safe_then(
      [ptrref=std::move(ptrref)]() mutable {
	return read_ertr::make_ready_future<bufferptr>(std::move(*ptrref));
      });
  }

  using write_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::ebadf,
    crimson::ct_error::enospc,
    crimson::ct_error::erange
    >;
  write_ertr::future<> write(extent_off_t addr, bufferptr &buf) {
    if (is_smr) {
      return crimson::ct_error::input_output_error::make();
    }
    else {
      return random_block_manager.write(addr, buf);
    }
  }

  /* Methods for discovering device geometry */
  size_t get_size() const {
    if (is_smr) {
      return segment_manager.get_size();
    }
  }
  extent_off_t get_block_size() const {
    if (is_smr) {
      return segment_manager.get_block_size();
    }
    else {
      return random_block_manager.get_block_size();
    }
  }
  extent_off_t get_allocation_unit_size() const {
    if (is_smr) {
      return segment_manager.get_segment_size();
    }
    else {
      return random_block_manager.get_block_size();
    }
  }
  extent_id_t get_num_allocation_units() const {
    ceph_assert(get_size() % get_allocation_unit_size() == 0);
    return ((extent_id_t)(get_size() / get_allocation_unit_size()));
  }
  const seastore_meta_t &get_meta() const {
    if (is_smr) {
      return segment_manager.get_meta();
    }
    else {
      return crimson::ct_error::input_output_error::make();
    }
  }

  ExtentAllocator(SegmentManagerRef segment_manager) :
  is_smr(true),
  segment_manager(segment_manager) {
  }

  ExtentAllocator(RandomBlockManagerRef random_block_manager) :
  is_smr(false),
  random_block_manager(random_block_manager) {
  }

  ~ExtentAllocator() {
  }

private:
  const bool is_smr;
  SegmentManagerRef segment_manager;
  RandomBlockManager random_block_manager;
};
using ExtentAllocatorRef = std::unique_ptr<ExtentAllocator>;

}
