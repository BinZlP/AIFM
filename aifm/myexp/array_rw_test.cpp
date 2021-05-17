extern "C" {
#include <runtime/runtime.h>
}

#include "array.hpp"
#include "device.hpp"
#include "helpers.hpp"
#include "manager.hpp"

#include "deref_scope.hpp"

#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <random>
#include <string>

#include <cstdlib>

using namespace far_memory;
using namespace std;

constexpr static uint64_t kCacheSize = (128ULL << 20);
constexpr static uint64_t kFarMemSize = (10ULL << 30);
constexpr static uint32_t kNumGCThreads = 40;
constexpr static uint32_t kNumEntries = ((1 << 27) * 10); // 2^30 / 8 elements
constexpr static uint32_t kNumConnections = 400;

//uint64_t raw_array_A[kNumEntries];
//uint64_t raw_array_B[kNumEntries];
//uint64_t raw_array_C[kNumEntries];

uint64_t *raw_array_A;
uint64_t *raw_array_B;

template <uint64_t N, typename T>
void copy_array(Array<T, N> *array, T *raw_array) {
  for (uint64_t i = 0; i < N; i++) {
    DerefScope scope;
    (*array).at_mut(scope, i) = raw_array[i];
  }
}

template <typename T, uint64_t N>
void add_array(Array<T, N> *array_C, Array<T, N> *array_A,
               Array<T, N> *array_B) {
  for (uint64_t i = 0; i < N; i++) {
    DerefScope scope;
    (*array_C).at_mut(scope, i) =
        (*array_A).at(scope, i) + (*array_B).at(scope, i);
  }
}

void gen_random_array(uint64_t num_entries, uint64_t *raw_array) {
  std::random_device rd;
  std::mt19937_64 eng(rd());
  std::uniform_int_distribution<uint64_t> distr;

  for (uint64_t i = 0; i < num_entries; i++) {
    raw_array[i] = distr(eng);
  }
}


template <typename T, uint64_t N>
void add_array_by_num(Array<T,N> *array, uint64_t num){
    for(uint64_t i = 0; i < N; i++) {
        DerefScope scope;
        (*array).at_mut(scope, i) = (*array).at_mut(scope, i) + num;
    }
}

void gen_initial_array(uint64_t num_entries, uint64_t *raw_array) {
  for (uint64_t i = 0; i < num_entries; i++)
    raw_array[i] = 1;  
}

void copy_array_local(uint64_t num_entries, uint64_t *src, uint64_t *dst) {
  for (uint64_t i = 0; i < num_entries; i++)
    dst[i] = src[i];
}

void add_array_local(uint64_t num_entries, uint64_t *arr1, uint64_t *arr2) {
  for (uint64_t i = 0; i < num_entries; i++)
    arr2[i] += arr1[i];
}

void do_work(FarMemManager *manager) {
  // Allocate heap for raw arrays
  raw_array_A = (uint64_t *)malloc(sizeof(uint64_t) * (uint64_t)kNumEntries);
  raw_array_B = (uint64_t *)malloc(sizeof(uint64_t) * (uint64_t)kNumEntries);
  if(raw_array_A == NULL || raw_array_B == NULL){
    cout << "Allcation failed" << endl;
    return;
  }

  // stopwatches
  std::chrono::time_point<std::chrono::steady_clock> times[10];

  // Far memory arrays
  auto array_A = manager->allocate_array<uint64_t, kNumEntries>();
  auto array_B = manager->allocate_array<uint64_t, kNumEntries>();
  //auto array_C = manager->allocate_array<uint64_t, kNumEntries>();

  // Generate random array of 10GB
  gen_random_array(kNumEntries, raw_array_A);
  gen_random_array(kNumEntries, raw_array_B);
  //gen_initial_array(kNumEntries, raw_array_B); // array for add 1 for every elements.


  // Copy generated array to far memory
  cout << "Step 1: copy random generated arrays to far memory" << endl;
  times[0] = std::chrono::steady_clock::now();
  copy_array(&array_A, raw_array_A);
  copy_array(&array_B, raw_array_B);
  times[1] = std::chrono::steady_clock::now();

  for (uint64_t i = 0; i < kNumEntries; i++) {
    DerefScope scope;
    if (array_A.at(scope, i) != raw_array_A[i])
      goto fail;
  }
  for (uint64_t i = 0; i < kNumEntries; i++) {
    DerefScope scope;
    if (array_B.at(scope, i) != raw_array_B[i])
      goto fail;
  }


  // Add 1 for all elements in array_A
  cout << "Step 2: add 1 for all elements" << endl;
  times[2] = std::chrono::steady_clock::now();
  add_array_by_num(&array_A, 1);
  times[3] = std::chrono::steady_clock::now();
  for (uint64_t i = 0; i < kNumEntries; i++)
    raw_array_A[i]++;


  // Read elements from array_A
  cout << "Step 3: read elements from far memory array A" << endl;
  times[4] = std::chrono::steady_clock::now();
  for (uint64_t i = 0; i < kNumEntries; i++) {
    DerefScope scope;
    if (array_A.at(scope, i) != raw_array_A[i]) {
      goto fail;
    }
  }
  times[5] = std::chrono::steady_clock::now();
  

  // A = A + B
  cout << "Step 4: add array B to array A" << endl;
  times[6] = std::chrono::steady_clock::now();
  add_array(&array_A, &array_A, &array_B);
  times[7] = std::chrono::steady_clock::now();
  for(uint64_t i=0; i<kNumEntries; i++)
    raw_array_A[i] += raw_array_B[i];

  // Read elements from array_A, again
  cout << "Step 5: finally read array A" << endl;
  times[8] = std::chrono::steady_clock::now();
  for (uint64_t i = 0; i < kNumEntries; i++) {
    DerefScope scope;
    if (array_A.at(scope, i) != raw_array_A[i]) {
      goto fail;
    }
  }
  times[9] = std::chrono::steady_clock::now();
  

  // Print out result
//  auto total_time = std::chrono::duration_cast<std::chrono::microseconds>(times[1] - times[0]).count();
//  for (uint64_t i = 2; i < size(times); i+=2)
//    total_time += std::chrono::duration_cast<std::chrono::microseconds>(times[i+1] - times[i]).count();
  for (uint32_t i = 0; i < std::size(times); i+=2) {
    cout << "Step" << i/2 + 1 << ": "
      << std::chrono::duration_cast<std::chrono::microseconds>(times[i+1] - times[i]).count()
      << " us" << endl;
  }
//  cout << "Total: " << total_time << " us" << endl;

  cout << endl << endl;

//   copy_array(&array_B, raw_array_B);
//   add_array(&array_C, &array_A, &array_B);

//   for (uint64_t i = 0; i < kNumEntries; i++) {
//     DerefScope scope;
//     if (array_C.at(scope, i) != raw_array_A[i] + raw_array_B[i]) {
//       goto fail;
//     }
//   }

  cout << "Passed" << endl;
  return;

fail:
  cout << "Failed" << endl;
}

int argc;
void _main(void *arg) {
  char **argv = static_cast<char **>(arg);
  std::string ip_addr_port(argv[1]);
  auto raddr = helpers::str_to_netaddr(ip_addr_port);
  std::unique_ptr<FarMemManager> manager =
      std::unique_ptr<FarMemManager>(FarMemManagerFactory::build(
          kCacheSize, kNumGCThreads,
          new TCPDevice(raddr, kNumConnections, kFarMemSize)));
  do_work(manager.get());
}

int main(int _argc, char *argv[]) {
  int ret;

  if (_argc < 3) {
    std::cerr << "usage: [cfg_file] [ip_addr:port]" << std::endl;
    return -EINVAL;
  }

  char conf_path[strlen(argv[1]) + 1];
  strcpy(conf_path, argv[1]);
  for (int i = 2; i < _argc; i++) {
    argv[i - 1] = argv[i];
  }
  argc = _argc - 1;

  ret = runtime_init(conf_path, _main, argv);
  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}

