//
// Created by zhou822 on 5/28/23.
//
#include "core/commit_manager.hpp"
#include <random>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring> // for memcpy
#include <cstdio>

namespace GTX{

    /*
     * the latch is held when the function is invoked, so the latch must be released here
     */
    void LatchDoubleBufferCommitManager::collaborative_commit(){
        commit_latch.lock();//synchronize commit groups
        uint8_t current_offset = offset;
        offset=1-offset;
        latch.unlock();
        //present the other queue to the worker threads
#if COMMIT_TEST
        if(double_buffer_queue[current_offset].queue_buffer.empty()){
            throw CommitException();
        }
#endif
        std::fill(std::begin(double_buffer_queue[current_offset].already_committed), std::end(double_buffer_queue[current_offset].already_committed), false);
        global_write_epoch++;
        while(!double_buffer_queue[current_offset].queue_buffer.empty()){
            auto current_entry = double_buffer_queue[current_offset].queue_buffer.front();
            //wal version adds a persist log step
            current_entry->status.store(global_write_epoch);
            double_buffer_queue[current_offset].queue_buffer.pop();
        }
        global_read_epoch.fetch_add(1);
        commit_latch.unlock();
    }

    void LatchDoubleBufferCommitManager::server_loop() {
        while(running.load()){
            latch.lock();
            commit_latch.lock();
            uint8_t current_offset = offset;
            offset=1-offset;
            latch.unlock();
            //skip empty buffer because there is no one to commit
            if(double_buffer_queue[current_offset].queue_buffer.empty()){
                commit_latch.unlock();
                continue;
            }
            std::fill(std::begin(double_buffer_queue[current_offset].already_committed), std::end(double_buffer_queue[current_offset].already_committed), false);
            global_write_epoch++;
            while(!double_buffer_queue[current_offset].queue_buffer.empty()){
                auto current_entry = double_buffer_queue[current_offset].queue_buffer.front();
                //wal version adds a persist log step
                current_entry->status.store(global_write_epoch);
                double_buffer_queue[current_offset].queue_buffer.pop();
            }
            global_read_epoch.fetch_add(1);
            commit_latch.unlock();
        }
        for(int i=0; i<=1; i++){
            if(double_buffer_queue[i].queue_buffer.empty()){
                continue;
            }
            global_write_epoch++;
            while(!double_buffer_queue[i].queue_buffer.empty()){
                auto current_entry = double_buffer_queue[i].queue_buffer.front();
                //wal version adds a persist log step
                current_entry->status.store(global_write_epoch);
                double_buffer_queue[i].queue_buffer.pop();
            }
            global_read_epoch.fetch_add(1);
        }
    }
    //Concurrent Array Commit Manager Server Loop
    void ConcurrentArrayCommitManager::server_loop() {
        std::uniform_int_distribution<> offset_distribution(0,current_writer_num-1);
        std::random_device rd; // obtain a random number from hardware
        std::mt19937 gen(rd());
        while(running.load(std::memory_order_acquire)){
            size_t commit_count =0;
            offset = offset_distribution(gen);
            uint32_t current_offset = offset;
            global_write_epoch++;
            do{
                auto current_txn_entry = commit_array[current_offset].txn_ptr.load(std::memory_order_acquire);
                if(current_txn_entry){
                    //Libin shifted their orders
                    commit_array[current_offset].txn_ptr.store(nullptr,std::memory_order_release);
                    current_txn_entry->status.store(global_write_epoch,std::memory_order_release);
                    //current_txn_entry->status.notify_one();
                    commit_count++;
                }
                current_offset = (current_offset+1)%current_writer_num;
            }while(current_offset!=offset);
            if(commit_count){
                global_read_epoch.fetch_add(1,std::memory_order_acq_rel);
            }else{
                global_write_epoch--;
            }
        }
        //now the stop running signal is sent
        global_write_epoch++;
        for(uint32_t i=0; i<current_writer_num;i++){
            auto current_txn_entry = commit_array[i].txn_ptr.load(std::memory_order_acquire);
            if(current_txn_entry){
                current_txn_entry->status.store(global_write_epoch);
                commit_array[i].txn_ptr.store(nullptr);
            }
        }
        global_read_epoch.fetch_add(1,std::memory_order_acq_rel);
    }
#if ENSURE_DURABILITY
    void ConcurrentArrayCommitManager::wal_server_loop() {
        std::filesystem::path logging_directory("/scratch1/zhou822/GTX_Durability/log_directory/");
        std::string wal_address = "/scratch1/zhou822/GTX_Durability/log_directory/wal.log";
        std::ofstream log_file(wal_address,std::ios::out | std::ios::app | std::ios::binary);
        std::uniform_int_distribution<> offset_distribution(0,current_writer_num-1);
        std::random_device rd; // obtain a random number from hardware
        std::mt19937 gen(rd());
        uint32_t group_log_counter = 0;
        while(running.load(std::memory_order_acquire)){
            size_t commit_count =0;
            offset = offset_distribution(gen);
            uint32_t current_offset = offset;
            global_write_epoch++;
            std::string group_log;
            group_log.append("start");
            //group_log.append(",");
            //group_log.append(std::to_string(global_write_epoch).append(","));
            group_log.append(std::string_view((char*)(&global_write_epoch),sizeof (global_write_epoch)));
            //let's just save that we don't do extra log, we do not increase read ts until logs are persisted.
            do{
                auto current_txn_entry = commit_array[current_offset].txn_ptr.load(std::memory_order_acquire);
                if(current_txn_entry){
                    group_log.append(current_txn_entry->get_wal());
                    current_txn_entry->clear_wal();
                    //Libin shifted their orders
                    commit_array[current_offset].txn_ptr.store(nullptr,std::memory_order_release);
                    current_txn_entry->status.store(global_write_epoch,std::memory_order_release);
                    //current_txn_entry->status.notify_one();
                    commit_count++;
                }
                current_offset = (current_offset+1)%current_writer_num;
            }while(current_offset!=offset);
            if(commit_count){
                //group_log.append(",");
                group_log.append("end");
                log_file<<group_log;
                group_log_counter++;
                global_read_epoch.fetch_add(1,std::memory_order_acq_rel);

                //occasionally allocate a new file
                if(group_log_counter==10000000){
                    std::string new_name = "/scratch1/zhou822/GTX_Durability/log_directory/wal_";
                    new_name.append(std::to_string(global_write_epoch));
                    log_file.close();
                    rename(wal_address.c_str(),new_name.c_str());//the new name of the old log file will contain timestamp information
                    log_file.open(wal_address,std::ios::out | std::ios::app | std::ios::binary);
                    group_log_counter=0;
                }

            }else{
                global_write_epoch--;
            }
        }
        //now the stop running signal is sent

        global_write_epoch++;
        /*std::string group_log;
        group_log.append("start");
        //group_log.append(std::to_string(global_write_epoch));
        group_log.append(std::string_view((char*)(&global_write_epoch),sizeof (global_write_epoch)));
        for(uint32_t i=0; i<current_writer_num;i++){
            auto current_txn_entry = commit_array[i].txn_ptr.load(std::memory_order_acquire);
            if(current_txn_entry){
                group_log.append(current_txn_entry->get_wal());
                current_txn_entry->status.store(global_write_epoch);
                commit_array[i].txn_ptr.store(nullptr);
            }
        }
        group_log.append("end");
        log_file<<group_log;*/
        log_file.close();
        global_read_epoch.fetch_add(1,std::memory_order_acq_rel);
        for (const auto& entry : std::filesystem::directory_iterator(logging_directory)){
            //std::filesystem::remove_all(entry.path());
        }

        //std::filesystem::remove(wal_address);//delete wal for our code, we have a graceful shutdown so no need for logs
    }

    void CommitManager ::mmap_wal_server_loop() {
        //constexpr auto chunk_size = (1u << 12u);//4kb
        uint64_t file_size = (1u << 29u);
        std::filesystem::path logging_directory("/scratch1/zhou822/GTX_Durability/log_directory/");
        std::string wal_address = "/scratch1/zhou822/GTX_Durability/log_directory/wal.log";
        int fd = open(wal_address.c_str(), O_RDWR | O_CREAT, 0666);
        if (fd == -1) {
            perror("Error opening file");
            return;
        }
        if (ftruncate(fd, file_size) == -1) {
            perror("Error resizing file");
            close(fd);
            return;
        }
        void* mappedData = mmap(nullptr, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        char* output_data = static_cast<char*>(mappedData);
        if (mappedData == MAP_FAILED) {
            perror("Error mapping file");
            close(fd);
            return;
        }
        std::uniform_int_distribution<> offset_distribution(0,current_writer_num-1);
        std::random_device rd; // obtain a random number from hardware
        std::mt19937 gen(rd());
        uint64_t log_offset = 0;
        while(running.load(std::memory_order_acquire)){
            size_t commit_count =0;
            offset = offset_distribution(gen);
            uint32_t current_offset = offset;
            global_write_epoch++;
            std::string group_log;
            group_log.append("start");
            group_log.append(",");
            group_log.append(std::to_string(global_write_epoch));
            //let's just save that we don't do extra log, we do not increase read ts until logs are persisted.
            do{
                auto current_txn_entry = commit_array[current_offset].txn_ptr.load(std::memory_order_acquire);
                if(current_txn_entry){
                    group_log.append(current_txn_entry->get_wal());
                    current_txn_entry->clear_wal();
                    //Libin shifted their orders
                    commit_array[current_offset].txn_ptr.store(nullptr,std::memory_order_release);
                    current_txn_entry->status.store(global_write_epoch,std::memory_order_release);
                    //current_txn_entry->status.notify_one();
                    commit_count++;
                }
                current_offset = (current_offset+1)%current_writer_num;
            }while(current_offset!=offset);
            if(commit_count){
                group_log.append(",");
                group_log.append("end");
                //allocate new file, reset values
                if((log_offset+group_log.size())>=file_size){
                    //force a synchronous flush
                    if (msync(mappedData, file_size, MS_SYNC) == -1)[[unlikely]] {
                        perror("Error syncing file");
                    }
                    if (munmap(mappedData, file_size) == -1) {
                        perror("Error unmapping file");
                    }
                    close(fd);
                    log_offset= 0;
                    //rename old file, open new file
                    std::string new_name = "/scratch1/zhou822/GTX_Durability/log_directory/wal_";
                    new_name.append(std::to_string(global_write_epoch-1));
                    rename(wal_address.c_str(),new_name.c_str());
                    fd = open(wal_address.c_str(), O_RDWR | O_CREAT, 0666);
                    if (fd == -1) {
                        perror("Error opening file");
                        return;
                    }
                    if (ftruncate(fd, file_size) == -1) {
                        perror("Error resizing file");
                        close(fd);
                        return;
                    }
                    mappedData = mmap(nullptr, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
                    output_data = static_cast<char*>(mappedData);
                }
                memcpy(&output_data[log_offset],group_log.c_str(),group_log.size());
                if (msync(mappedData, file_size, MS_SYNC) == -1)[[unlikely]] {
                    perror("Error syncing file");
                }
                log_offset+=group_log.size();
                global_read_epoch.fetch_add(1,std::memory_order_acq_rel);
            }else{
                global_write_epoch--;
            }
        }
        //graceful shutdown
        global_write_epoch++;
        std::string group_log;
        group_log.append("start");
        group_log.append(std::to_string(global_write_epoch));
        for(uint32_t i=0; i<current_writer_num;i++){
            auto current_txn_entry = commit_array[i].txn_ptr.load(std::memory_order_acquire);
            if(current_txn_entry){
                group_log.append(current_txn_entry->get_wal());
                current_txn_entry->status.store(global_write_epoch);
                commit_array[i].txn_ptr.store(nullptr);
            }
        }
        group_log.append("end");

        if((log_offset+group_log.size())>=file_size){
            //force a synchronous flush
            if (msync(mappedData, file_size, MS_SYNC) == -1)[[unlikely]] {
                perror("Error syncing file");
            }
            if (munmap(mappedData, file_size) == -1) {
                perror("Error unmapping file");
            }
            close(fd);
            log_offset= 0;
            //rename old file, open new file
            std::string new_name = "/scratch1/zhou822/GTX_Durability/log_directory/wal_";
            new_name.append(std::to_string(global_write_epoch-1));
            rename(wal_address.c_str(),new_name.c_str());
            fd = open(wal_address.c_str(), O_RDWR | O_CREAT, 0666);
            if (fd == -1) {
                perror("Error opening file");
                return;
            }
            if (ftruncate(fd, file_size) == -1) {
                perror("Error resizing file");
                close(fd);
                return;
            }
            mappedData = mmap(nullptr, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
            output_data = static_cast<char*>(mappedData);
        }
        memcpy(&output_data[log_offset],group_log.c_str(),group_log.size());
        if (msync(mappedData, file_size, MS_SYNC) == -1)[[unlikely]] {
            perror("Error syncing file");
        }
        //close file
        if (munmap(mappedData, file_size) == -1) {
            perror("Error unmapping file");
        }
        close(fd);
        global_read_epoch.fetch_add(1,std::memory_order_acq_rel);
        /*for (const auto& entry : std::filesystem::directory_iterator(logging_directory))
            std::filesystem::remove_all(entry.path());*/
    }

    void CommitManager::write_to_buffer(GTX::ConcurrentArrayCommitManager::log_buffer &buffers,std::string& data,std::ofstream& log_file) {
        if(data.size()+buffers.buffer_offset>buffer_size){
            uint64_t part1_size = buffer_size - buffers.buffer_offset;
            uint64_t part2_size = data.size()-part1_size;
            std::memcpy(&buffers.buffers[buffers.buffer_index][buffers.buffer_offset],data.c_str(),part1_size);
            //log_file<<buffers.buffers[buffers.buffer_index];
            //log_file.write(reinterpret_cast<char*>(buffers.buffers[buffers.buffer_index]),buffer_size);
            //log_file.write(buffers.buffers[buffers.buffer_index],buffer_size);
            log_file<<std::string_view(buffers.buffers[buffers.buffer_index],buffer_size);
            buffers.buffer_index=(buffers.buffer_index+1)%3;
            buffers.buffer_offset=0;
            if(part2_size<=buffer_size)[[likely]]{
                memcpy(&buffers.buffers[buffers.buffer_index][buffers.buffer_offset],&data.c_str()[part1_size],part2_size);
                buffers.buffer_offset+=part2_size;
            }else{
                while(part2_size>0){
                    if(part2_size<=buffer_size){
                        memcpy(&buffers.buffers[buffers.buffer_index][buffers.buffer_offset],&data.c_str()[part1_size],part2_size);
                        buffers.buffer_offset+=part2_size;
                        part2_size=0;
                    }else{
                        memcpy(&buffers.buffers[buffers.buffer_index][buffers.buffer_offset],&data.c_str()[part1_size],buffer_size);
                        part1_size+=buffer_size;
                        //log_file<<buffers.buffers[buffers.buffer_index];
                        //log_file.write(buffers.buffers[buffers.buffer_index],buffer_size);
                        log_file<<std::string_view(buffers.buffers[buffers.buffer_index],buffer_size);
                        buffers.buffer_index=(buffers.buffer_index+1)%3;
                        part2_size-=buffer_size;
                    }
                }
            }

                /*uint64_t part1_size = buffer_size - buffers.buffer_offset;
                uint64_t part2_size = data.size()-part1_size;
                memcpy(&buffers.buffers[buffers.buffer_index][buffers.buffer_offset],data.c_str(),part1_size);
                log_file<<buffers.buffers[buffers.buffer_index];
                buffers.buffer_index=(buffers.buffer_index+1)%3;
                buffers.buffer_offset=0;
                memcpy(&buffers.buffers[buffers.buffer_index][buffers.buffer_offset],&data.c_str()[part1_size],part2_size);
                buffers.buffer_offset+=part2_size;*/

        }else{
            memcpy(&buffers.buffers[buffers.buffer_index][buffers.buffer_offset],data.c_str(),data.size());
            buffers.buffer_offset+=data.size();
        }
    }
    void CommitManager::asynchronous_wal_loop() {
        std::filesystem::path logging_directory("/scratch1/zhou822/GTX_Durability/log_directory/");
        std::string wal_address = "/scratch1/zhou822/GTX_Durability/log_directory/wal.log";
        std::ofstream log_file(wal_address,std::ios::out | std::ios::app | std::ios::binary);
        std::uniform_int_distribution<> offset_distribution(0,current_writer_num-1);
        std::random_device rd; // obtain a random number from hardware
        std::mt19937 gen(rd());
        uint32_t group_log_counter = 0;
        log_buffer log_buffers;

        std::string log_suffix = "end";
        while(running.load(std::memory_order_acquire)){
            size_t commit_count =0;
            offset = offset_distribution(gen);
            uint32_t current_offset = offset;
            global_write_epoch++;
            //std::string group_log;
            //group_log.append("start");
            //group_log.append(",");
            //group_log.append(std::to_string(global_write_epoch));
            //let's just save that we don't do extra log, we do not increase read ts until logs are persisted.
            bool first_txn_entry=true;
            do{
                auto current_txn_entry = commit_array[current_offset].txn_ptr.load(std::memory_order_acquire);
                if(current_txn_entry){
                    if(first_txn_entry){
                        std::string log_prefix = "start";
                        log_prefix.append(std::string_view((char*)(&global_write_epoch),sizeof (global_write_epoch)));
                        //log_prefix.append(std::to_string(global_write_epoch));
                        //log_prefix.append(",");
                        write_to_buffer(log_buffers,log_prefix,log_file);
                        first_txn_entry=false;
                    }
                    write_to_buffer(log_buffers,current_txn_entry->get_wal(),log_file);
                    //group_log.append(current_txn_entry->get_wal());
                    current_txn_entry->clear_wal();
                    //Libin shifted their orders
                    commit_array[current_offset].txn_ptr.store(nullptr,std::memory_order_release);
                    current_txn_entry->status.store(global_write_epoch,std::memory_order_release);
                    //current_txn_entry->status.notify_one();
                    commit_count++;
                }
                current_offset = (current_offset+1)%current_writer_num;
            }while(current_offset!=offset);
            if(commit_count){
                //group_log.append(",");
                //group_log.append("end");
                //log_file<<group_log;
                //need io
                /*if(buffer_offset+group_log.size()>=buffer_size){
                    //break into 2 buffers

                    buffer_index = (buffer_index+1)%3;
                }else{

                }*/
                write_to_buffer(log_buffers,log_suffix,log_file);
                group_log_counter++;
                global_read_epoch.fetch_add(1,std::memory_order_acq_rel);

                //occasionally allocate a new file
                if(group_log_counter==10000000){
                    //flush remaining data in the current buffer, reset the buffer
                    if(log_buffers.buffer_offset>0){
                        //log_file.write(reinterpret_cast<char*>(log_buffers.buffers[log_buffers.buffer_index]),log_buffers.buffer_offset);
                        log_file<<std::string_view (log_buffers.buffers[log_buffers.buffer_index],log_buffers.buffer_offset);
                        log_buffers.buffer_index=0;
                        log_buffers.buffer_offset=0;
                    }
                    std::string new_name = "/scratch1/zhou822/GTX_Durability/log_directory/wal_";
                    new_name.append(std::to_string(global_write_epoch));
                    log_file.close();
                    rename(wal_address.c_str(),new_name.c_str());//the new name of the old log file will contain timestamp information
                    log_file.open(wal_address,std::ios::out | std::ios::app | std::ios::binary);
                    group_log_counter=0;
                }

            }else{
                global_write_epoch--;
            }
        }
        //now the stop running signal is sent, write the last bytes in buffer, and do the shutdown
        if(log_buffers.buffer_offset>0){
            //log_file.write(reinterpret_cast<char*>(log_buffers.buffers[log_buffers.buffer_index]),log_buffers.buffer_offset);
            log_file<<std::string (log_buffers.buffers[log_buffers.buffer_index],log_buffers.buffer_offset);
            log_buffers.buffer_index=0;
            log_buffers.buffer_offset=0;
        }
        /*
        global_write_epoch++;
        std::string group_log;
        group_log.append("start");
        group_log.append(std::string_view((char*)(&global_write_epoch),sizeof (global_write_epoch)));
        //group_log.append(std::to_string(global_write_epoch));
        //group_log.append(",");
        for(uint32_t i=0; i<current_writer_num;i++){
            auto current_txn_entry = commit_array[i].txn_ptr.load(std::memory_order_acquire);
            if(current_txn_entry){
                group_log.append(current_txn_entry->get_wal());
                current_txn_entry->status.store(global_write_epoch);
                commit_array[i].txn_ptr.store(nullptr);
            }
        }
        group_log.append("end");
        log_file<<group_log;
        log_file.close();
        global_read_epoch.fetch_add(1,std::memory_order_acq_rel);*/
        for (const auto& entry : std::filesystem::directory_iterator(logging_directory)) {
            //std::filesystem::remove_all(entry.path());
        }
    }

    void CommitManager::simplified_asynchronous_wal_loop() {
        std::filesystem::path logging_directory("/scratch1/zhou822/GTX_Durability/log_directory/");
        std::string wal_address = "/scratch1/zhou822/GTX_Durability/log_directory/wal.log";
        std::ofstream log_file(wal_address,std::ios::out | std::ios::app | std::ios::binary);
        std::uniform_int_distribution<> offset_distribution(0,current_writer_num-1);
        std::random_device rd; // obtain a random number from hardware
        std::mt19937 gen(rd());
        uint32_t group_log_counter = 0;
        std::string group_log;
        group_log.reserve((1ul<<19)+(1ul<<11));
        while(running.load(std::memory_order_acquire)){
            size_t commit_count =0;
            offset = offset_distribution(gen);
            uint32_t current_offset = offset;
            global_write_epoch++;
            std::string local_group_log("start,");
            //group_log.append("start");
            //group_log.append(",");
            local_group_log.append(std::to_string(global_write_epoch).append(","));
            //let's just save that we don't do extra log, we do not increase read ts until logs are persisted.
            do{
                auto current_txn_entry = commit_array[current_offset].txn_ptr.load(std::memory_order_acquire);
                if(current_txn_entry){
                    local_group_log.append(current_txn_entry->get_wal());
                    current_txn_entry->clear_wal();
                    //Libin shifted their orders
                    commit_array[current_offset].txn_ptr.store(nullptr,std::memory_order_release);
                    current_txn_entry->status.store(global_write_epoch,std::memory_order_release);
                    //current_txn_entry->status.notify_one();
                    commit_count++;
                }
                current_offset = (current_offset+1)%current_writer_num;
            }while(current_offset!=offset);
            if(commit_count){
                group_log.append(local_group_log);
                group_log.append(",");
                group_log.append("end");
                if(group_log.size()>(1ul<<19)){
                    log_file<<group_log;
                    group_log.clear();
                }
                //log_file<<group_log;
                group_log_counter++;
                global_read_epoch.fetch_add(1,std::memory_order_acq_rel);

                //occasionally allocate a new file
                if(group_log_counter==10000000){
                    if(group_log.size()>0){
                        log_file<<group_log;
                        group_log.clear();
                    }
                    std::string new_name = "/scratch1/zhou822/GTX_Durability/log_directory/wal_";
                    new_name.append(std::to_string(global_write_epoch));
                    log_file.close();
                    rename(wal_address.c_str(),new_name.c_str());//the new name of the old log file will contain timestamp information
                    log_file.open(wal_address,std::ios::out | std::ios::app | std::ios::binary);
                    group_log_counter=0;
                }

            }else{
                global_write_epoch--;
            }
        }
        //now the stop running signal is sent
        global_write_epoch++;
        //std::string group_log;
        group_log.append("start");
        group_log.append(std::to_string(global_write_epoch));
        for(uint32_t i=0; i<current_writer_num;i++){
            auto current_txn_entry = commit_array[i].txn_ptr.load(std::memory_order_acquire);
            if(current_txn_entry){
                group_log.append(current_txn_entry->get_wal());
                current_txn_entry->status.store(global_write_epoch);
                commit_array[i].txn_ptr.store(nullptr);
            }
        }
        group_log.append("end");
        log_file<<group_log;
        log_file.close();
        global_read_epoch.fetch_add(1,std::memory_order_acq_rel);
        for (const auto& entry : std::filesystem::directory_iterator(logging_directory)){
            //std::filesystem::remove_all(entry.path());
        }

    }
#endif
}