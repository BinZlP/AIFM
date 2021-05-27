#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <chrono>
#include <atomic>
#include <thread>

using namespace std;

constexpr uint64_t kCacheMBs = 8192;
constexpr static uint64_t kCacheSize = kCacheMBs << 20;
constexpr static uint64_t kFarMemSize = (40ULL << 30);
constexpr static uint32_t kNumGCThreads = 12;
constexpr static uint32_t kNumEntries = (1024ULL << 20); // 2^30 elements
constexpr static uint32_t kNumConnections = 400;

uint64_t *raw_array_A;
uint64_t *raw_array_B;
//uint64_t raw_array_C[kNumEntries];

// template <uint64_t N, typename T>
// void copy_array(Array<T, N> *array, T *raw_array) {
//   for (uint64_t i = 0; i < N; i++) {
//     DerefScope scope;
//     (*array).at_mut(scope, i) = raw_array[i];
//   }
// }

// template <typename T, uint64_t N>
// void add_array(Array<T, N> *array_C, Array<T, N> *array_A,
//                Array<T, N> *array_B) {
//   for (uint64_t i = 0; i < N; i++) {
//     DerefScope scope;
//     (*array_C).at_mut(scope, i) =
//         (*array_A).at(scope, i) + (*array_B).at(scope, i);
//   }
// }

void gen_random_array(uint64_t num_entries, uint64_t *raw_array) {
  std::random_device rd;
  std::mt19937_64 eng(rd());
  std::uniform_int_distribution<uint64_t> distr;

  for (uint64_t i = 0; i < num_entries; i++) {
    raw_array[i] = distr(eng);
  }
}


// template <typename T, uint64_t N>
// void add_array_by_num(Array<T,N> *array, uint64_t num){
//     for(uint64_t i = 0; i < N; i++) {
//         DerefScope scope;
//         (*array).at_mut(scope, i) = (*array).at_mut(scope, i) + num;
//     }
// }

atomic_uint64_t i;

void gen_initial_array(uint64_t num_entries, uint64_t *raw_array) {
  for (; ++i < num_entries;)
    raw_array[i] = 0;
}

void copy_array_local(uint64_t num_entries, uint64_t *src, uint64_t *dst) {
  for (; ++i < num_entries;)
    dst[i] = src[i];
}

void add_array_local(uint64_t num_entries, uint64_t *arr1, uint64_t *arr2) {
  for (; ++i < num_entries;)
    arr2[i] += arr1[i];
}

void add_array_by_num_local(uint64_t num_entries, uint64_t *arr, uint64_t num){
  for (; ++i < num_entries;)
    arr[i] += num;
}

void read_and_compare(uint64_t num_entries, uint64_t *arr){
  uint64_t tmp=0;
  for (; ++i < num_entries;)
    if(tmp==arr[i]);
}

int main(int _argc, char *argv[]) {
  // stopwatches & threads
  std::chrono::time_point<std::chrono::steady_clock> times[10];
  std::thread threads[8];

  // Far memory arrays
  //auto array_A = manager->allocate_array<uint64_t, kNumEntries>();
  //auto array_B = manager->allocate_array<uint64_t, kNumEntries>();
  //auto array_C = manager->allocate_array<uint64_t, kNumEntries>();

  // Allocate heap for raw arrays
  raw_array_A = (uint64_t *)malloc(sizeof(uint64_t) * (uint64_t)kNumEntries);
  raw_array_B = (uint64_t *)malloc(sizeof(uint64_t) * (uint64_t)kNumEntries);
  if(raw_array_A == NULL || raw_array_B == NULL){
    cout << "Allcation failed" << endl;
    return -1;
  }

  // Generate random array of 10GB
  gen_initial_array(kNumEntries, raw_array_A);
  gen_random_array(kNumEntries, raw_array_B);
  //gen_initial_array(kNumEntries, raw_array_B); // array for add 1 for every elements.


  // Copy generated array to far memory
  cout << "Step 1: copy random generated arrays to far memory" << endl;
  i = 0;
  for (uint64_t i = 0; i < 8 ; i++)
    threads[i] = std::thread(copy_array_local, kNumEntries, raw_array_B, raw_array_A);
  times[0] = std::chrono::steady_clock::now();
  //copy_array_local(kNumEntries, raw_array_B, raw_array_A);
  for (uint64_t i = 0; i < 8 ; i++)
    threads[i].join();
  times[1] = std::chrono::steady_clock::now();


  // Add 1 for all elements in array_A
  cout << "Step 2: add 1 for all elements" << endl;
  i = 0;
  for (uint64_t i = 0; i < 8 ; i++)
    threads[i] = std::thread(add_array_by_num_local, kNumEntries, raw_array_A, 1);
  times[2] = std::chrono::steady_clock::now();
  //add_array_by_num_local(kNumEntries, raw_array_A, 1);
  for (uint64_t i = 0; i < 8 ; i++)
    threads[i].join();
  times[3] = std::chrono::steady_clock::now();
  for (uint64_t i = 0; i < kNumEntries; i++)
    raw_array_A[i]++;


  // Read elements from array_A
  cout << "Step 3: read elements from far memory array A" << endl;
  i = 0;
  for (uint64_t i = 0; i < 8 ; i++)
    threads[i] = std::thread(read_and_compare, kNumEntries, raw_array_A);
  times[4] = std::chrono::steady_clock::now();
  //uint64_t read_tmp = 0;
  //for(uint64_t i = 0; i < kNumEntries; i++)
  //  if(read_tmp == raw_array_A[i]);
  for (uint64_t i = 0; i < 8 ; i++)
    threads[i].join();
  times[5] = std::chrono::steady_clock::now();
  

  // A = A + B
  cout << "Step 4: add array B to array A" << endl;
  i = 0;
  for (uint64_t i = 0; i < 8 ; i++)
    threads[i] = std::thread(add_array_local, kNumEntries, raw_array_B, raw_array_A);
  times[6] = std::chrono::steady_clock::now();
  //for(uint64_t i=0; i<kNumEntries; i++)
  //  raw_array_A[i] += raw_array_B[i];
  for (uint64_t i = 0; i < 8 ; i++)
    threads[i].join();
  times[7] = std::chrono::steady_clock::now();


  // Read elements from array_A, again
  cout << "Step 5: finally read array A" << endl;
  i = 0;
  for (uint64_t i = 0; i < 8 ; i++)
    threads[i] = std::thread(read_and_compare, kNumEntries, raw_array_A);
  times[8] = std::chrono::steady_clock::now();
  //for(uint64_t i = 0; i < kNumEntries; i++)
  //  if(read_tmp == raw_array_A[i]);
  for (uint64_t i = 0; i < 8 ; i++)
    threads[i].join();
  times[9] = std::chrono::steady_clock::now();
  

  // Print out result
  auto total_time = std::chrono::duration_cast<std::chrono::microseconds>(times[1] - times[0]).count();
  for (uint64_t i = 2; i < 10; i+=2)
    total_time += std::chrono::duration_cast<std::chrono::microseconds>(times[i+1] - times[i]).count();
  for (uint32_t i = 0; i < 10; i+=2) {
    cout << "Step" << i/2 + 1 << ": "
      << std::chrono::duration_cast<std::chrono::microseconds>(times[i+1] - times[i]).count()
      << " us" << endl;
  }
  cout << "Total: " << total_time << " us" << endl;

  cout << endl << endl;

  return 0;
}

