
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <iostream>
#include <ctime>
#include <fstream>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <Python.h>

namespace py = pybind11;

using namespace std;

int input_len = 6;
float bandwidthLimit = (1000000. / 8.) * 60. * 60. * 24;
float it_cpueff_diff = 19;
float us_cpueff_diff = 10;
float it_maxsize = 47585.251;
float it_minsize = 0.105;
float it_mean_size = 3397.512895452965;
float it_stdev_size = 2186.2590964080405;
float it_limsup_size = it_mean_size + it_stdev_size;
float it_liminf_size = it_mean_size - it_stdev_size;
float it_delta_size = it_limsup_size - it_liminf_size;

void DatePlusDays( struct tm* date, int days )
{
    const time_t ONE_DAY = 24 * 60 * 60 ;

    // Seconds since start of epoch
    time_t date_seconds = mktime( date ) + (days * ONE_DAY) ;

    // Update caller's date
    // Use localtime because mktime converts to UTC so may change date
    *date = *localtime( &date_seconds ) ; ;
}

class WindowElement{
    public:
        int counter;
        vector<float> cur_values;
        vector<float> next_values;
        float reward;
        int action;

        WindowElement(int c, vector<float> c_v, float r, int a){
            counter = c;
            cur_values = c_v;
            reward = r;
            action = a;
        };
    
        vector<float> concat_with_next_values(float occupancy, float hit_rate){

            next_values.push_back(cur_values[0]);
            next_values.push_back(cur_values[1] + 1);
            next_values.push_back(cur_values[2]);
            next_values.push_back(cur_values[3]); 
            next_values.push_back(occupancy/100.);
            next_values.push_back(hit_rate);

            vector<float> reward_vector;
            vector<float> action_vector;
            reward_vector.push_back(reward);
            action_vector.push_back(action);
            vector<float> result;
            result.reserve(cur_values.size() + 1 + 1 + next_values.size()); // preallocate memory
            
            result.insert( result.end(), cur_values.begin(), cur_values.end() );
            result.insert( result.end(), action_vector.begin(), action_vector.end() );
            result.insert( result.end(), reward_vector.begin(), reward_vector.end() );
            result.insert( result.end(), next_values.begin(), next_values.end() );

            return result; 
    };
};

struct FileStats {
    float _size; 
    int _hit;
    int _miss;
    int _last_request;
    int _datatype;
};

class Stats {
    public:
        unordered_map<int, FileStats> _files;
        
        inline FileStats get_or_set(int filename, float size, int datatype, int request){
        bool found = false;
        FileStats stats;
        unordered_map<int,FileStats>::iterator it;
        it = _files.find(filename);
        if(it != _files.end())
            found = true;
        if(found == false){
            stats._size = size;
            stats._hit = 0;
            stats._miss = 0;
            stats._last_request = request;
            stats._datatype = datatype;
            _files[filename] = stats;
        }
        else
            stats = _files[filename];

        return stats;
        };
};

class cache {
    public: 
        unordered_set<int> _cached_files;
        vector<int> _cached_files_keys;
        vector<float> _daily_rewards_add;
        vector<float> _daily_rewards_evict;
        Stats _stats;
        float _size, _max_size;
        int _hit, _miss, _daily_anomalous_CPUeff_counter;
        float _written_data,_deleted_data,_read_data, _dailyReadOnHit, _dailyReadOnMiss, _daily_reward, _CPUeff, _h_watermark, _l_watermark;
        cache(){
            _size = 0.0;
            _max_size = 0.;
            _hit = 0;
            _miss = 0;
            _written_data = 0.0;
            _deleted_data = 0.0;
            _read_data = 0.0;
            _dailyReadOnHit = 0.0;
            _dailyReadOnMiss = 0.0;
            _daily_reward = 0.0;
            _daily_anomalous_CPUeff_counter = 0;
            _CPUeff = 0.0;
            _h_watermark = 95.;
            _l_watermark = 0.;
            cout<<'CREATED CACHE WITH SIZE ' << endl;
        };
        inline float capacity() { return (_size / _max_size) * 100.; };
        inline float hit_rate() { 
            if (_hit != 0.)
                return _hit / (_hit + _miss);
            return 0.;
        };
        inline FileStats before_request(int filename, bool hit, float size, int datatype, int request){
            FileStats stats;
            stats = _stats.get_or_set(filename, size, datatype, request);  
            if (hit == true) 
                stats._hit += 1;
            else
                stats._miss += 1;     
            return stats;  
        };               
        inline bool update_policy(int filename, FileStats file_stats, bool hit, int action){   
            _stats._files[filename] = file_stats;
            if (hit == false and action == 0){
                _cached_files.insert(filename);
                return true;
            }
            else if (hit == true)
                return false;
        };
        inline void after_request(FileStats fileStats, bool hit, bool added){
            if (hit == true){
                _hit += 1;
                _dailyReadOnHit += fileStats._size;
            }
            else{
                _miss += 1;
                _dailyReadOnMiss += fileStats._size;
            }
            if (added == true){
                _size += fileStats._size;
                _written_data += fileStats._size;
            }
            _read_data += fileStats._size;
        };
        inline float _get_mean_recency(int curRequest_from_start){
            if (_cached_files.size() == 0)
                return 0.;
            else{
                float mean = 0;
                int counter = 0;
                for (const auto& filename: _cached_files) {
                    mean += curRequest_from_start - _stats._files[filename]._last_request;
                    counter += 1;
                }
                return mean/float(counter);
            }
        };
        inline float _get_mean_frequency(int curRequest_from_start){
            if (_cached_files.size() == 0)
                return 0.;
            else{
                float mean = 0;
                int counter = 0;
                for (const auto& filename: _cached_files) {
                mean += _stats._files[filename]._hit + _stats._files[filename]._miss;
                counter += 1;
                }
                return mean/float(counter);
            }
        };
        inline float _get_mean_size(int curRequest_from_start){
            if (_cached_files.size() == 0)
                return 0.;
            else{
                float mean = 0;
                int counter = 0;
                for (const auto& filename: _cached_files) {
                 mean += _stats._files[filename]._size;
                counter += 1;
                }
                return mean/float(counter);
            }
        };
};

class env{
    public:
        int _startMonth, _endMonth, _time_span_add, _time_span_evict, _purge_delta, _output_activation, _seed, _df_length;
        string _directory, _out_directory, _out_name;
        float _cache_size, _size_tot;
        int _idx_start, _idx_end, _curDay, _totalDays, _adding_or_evicting, _curRequest, _curRequest_from_start, _cached_files_index;
        cache _cache;
        unordered_map<int,vector<WindowElement>> _request_window_elements, _eviction_window_elements;
        vector<float> _curValues;
        vector<vector<float>> _add_memory_vector;
        vector<vector<float>> _evict_memory_vector;        
        env(int start_month, int end_month, string directory, string out_directory, string out_name, int time_span_add, int time_span_evict, int purge_delta, string output_activation, float cache_size, int seed){
            _startMonth = start_month;
            _endMonth = end_month;
            _directory = directory;
            _out_directory = out_directory;
            _out_name = out_name;
            _time_span_add = time_span_add; 
            _time_span_evict = time_span_evict; 
            _purge_delta = purge_delta;
            _seed = seed;
            if (output_activation == "sigmoid") _output_activation = 0; else _output_activation = 1;
            _cache._max_size = cache_size;

            struct tm start_date = { 0, 0, 12 } ;  // nominal time midday (arbitrary).
            start_date.tm_year = 2018 - 1900 ;
            start_date.tm_mon = 1 - 1 ;  // note: zero indexed
            start_date.tm_mday = 1 ;       // note: not zero indexed

            struct tm end_date = { 0, 0, 12 } ;  // nominal time midday (arbitrary).
            end_date.tm_year = 2018 - 1900 ;
            end_date.tm_mon = 1 - 1 ;  // note: zero indexed
            end_date.tm_mday = 1 ;       // note: not zero indexed
            
            int idx_start = 0;
            while(start_date.tm_mon + 1 != start_month){
                idx_start += 1;
                DatePlusDays( &start_date, +1 ); 
            }
            int idx_end = idx_start;
            if (end_month != 12){
                while (end_date.tm_mon + 1 != end_month + 1){
                    idx_end += 1;
                    cout<<end_date.tm_mday<<' '<<end_date.tm_mon<<endl;
                    DatePlusDays( &end_date, +1 ); 
                }    
            }
            else{
                while(end_date.tm_mon != 1){
                    idx_end += 1;
                    cout<<end_date.tm_mday<<' '<<end_date.tm_mon<<endl;
                    DatePlusDays( &end_date, +1 ); 
                }
            }
            _idx_start = idx_start;
            _idx_end = idx_end;
            _curDay = idx_start ;
            _totalDays = idx_end - idx_start;

            _adding_or_evicting = 0;
            _size_tot = 0;

            _curRequest = -1;
            _curRequest_from_start = -1;
            _cached_files_index = -1;

        };
        void update_windows_getting_eventual_rewards_accumulate(int curFilename, int action);
        void look_for_invalidated_add_evict_accumulate();
        void purge();
        void set_curValues(float size, float frequency, float recency, float datatype, float occupancy, float hit_rate);
        py::array get_random_batch(int);
        bool check(int);
        FileStats get_stats(int);
        void delete_first_add_memory();
        void delete_first_evict_memory();
        int get_add_memory_size();
        int get_evict_memory_size();
        bool check_in_cache(int);
};

void env::update_windows_getting_eventual_rewards_accumulate(int curFilename, int action){
    if (_adding_or_evicting == 0){
        float size = _curValues[0];
        float coeff;
        if (_output_activation == 1) coeff = size;
        else{
            if (size <= it_liminf_size) coeff = 0;
            else if (size >= it_limsup_size) coeff = 1;
            else coeff = (size - it_liminf_size)/it_delta_size;
        }

        unordered_map<int,vector<WindowElement>>::iterator it;
        it = _request_window_elements.find(curFilename);
        if(it != _request_window_elements.end()){  //is pending
            for(int i=0; i < it->second.size(); i++){
                WindowElement obj = (it->second)[i];
                
                if ((_curRequest_from_start - obj.counter) >= _time_span_add){ //is invalidated
                    if (obj.reward != 0){   //some hits
                        if (obj.action == 0) obj.reward = + obj.reward * coeff;
                        else obj.reward = - obj.reward * coeff;
                    }
                    else{                //no hits at all
                        if (obj.action == 0) obj.reward = - 1 * coeff;
                        else obj.reward = + 1 * coeff;
                    }
                    _cache._daily_rewards_add.push_back(obj.reward);
                    vector<float> to_add = obj.concat_with_next_values(_cache.capacity(), _cache.hit_rate());
                    _add_memory_vector.push_back(to_add);
                    _request_window_elements[curFilename].erase(_request_window_elements[curFilename].begin() + i);
                }
                else{  //is not invalidated yet
                    obj.reward += 1;
                    _request_window_elements[curFilename].push_back(WindowElement(_curRequest_from_start, _curValues, 0, action));
                }
            }
        }
        
        else{ //is not in not pending
            WindowElement to_add_obj(_curRequest_from_start, _curValues, float(0.), action); 
            vector<WindowElement> to_add_vector;
            to_add_vector.push_back(to_add_obj);
            _request_window_elements[curFilename] = to_add_vector;
        }
        //######### GIVING REWARD TO EVICTION AND REMOVING FROM WINDOW ################################################################################################

        //unordered_map<int,vector<WindowElement>>::iterator it;
        it = _eviction_window_elements.find(curFilename);
        if(it != _eviction_window_elements.end()){  //is pending
            for(int i=0; i < it->second.size(); i++){
                WindowElement obj = it->second[i];

                if ((_curRequest_from_start - obj.counter) >= _time_span_evict){ //is invalidated
                    if (obj.reward != 0){   //some hits
                        if (obj.action == 0) obj.reward = + obj.reward * coeff;
                        else obj.reward = - obj.reward * coeff;
                    }
                    else{                //no hits at all
                        if (obj.action == 0) obj.reward = - 1 * coeff;
                        else obj.reward = + 1 * coeff;
                    }
                    _cache._daily_rewards_evict.push_back(obj.reward);
                    vector<float> to_add = obj.concat_with_next_values(_cache.capacity(), _cache.hit_rate());
                    _evict_memory_vector.push_back(to_add);
                    _eviction_window_elements[curFilename].erase(_request_window_elements[curFilename].begin() + i);
                }

                else  //is not invalidated yet
                    obj.reward += 1;
            } 
        }
    }

    else if (_adding_or_evicting == 1){
            WindowElement to_add(_curRequest_from_start, _curValues, 0, action);
            unordered_map<int,vector<WindowElement>>::iterator it;
            it = _eviction_window_elements.find(curFilename);

            if (it != _eviction_window_elements.end()){
                vector<WindowElement> to_add_vector; 
                to_add_vector.push_back(to_add);
                _eviction_window_elements[curFilename] = to_add_vector;
            }
            else (it->second).push_back(to_add);
        }
};

void env::look_for_invalidated_add_evict_accumulate(){
    unordered_set<int> toDelete;
    float coeff;
    for (auto const& elem: _request_window_elements){
        for(int i=0; i < elem.second.size(); i++){
            int curFilename = elem.first;
            WindowElement obj = elem.second[i];
            float size = obj.cur_values[0];
            if (_output_activation == 1) coeff = size;
            else{
                if (size <= it_liminf_size) float coeff = 0; 
                else if (size >= it_limsup_size) float coeff = 1; 
                else float coeff = (size - it_liminf_size)/it_delta_size;
            }
            
            if ((_curRequest_from_start - obj.counter) > _time_span_add){
                if (obj.reward != 0){  //some hits
                    if(obj.action == 0) obj.reward = + obj.reward * coeff;
                    else obj.reward = - obj.reward * coeff;
                }
                else{                  //no hits at all
                    if (obj.action == 0) obj.reward = - 1 * coeff;
                    else obj.reward = + 1 * coeff;
                }
                _cache._daily_rewards_add.push_back(obj.reward);
                vector<float> to_add = obj.concat_with_next_values(_cache.capacity(), _cache.hit_rate());
                _add_memory_vector.push_back(to_add);
                _request_window_elements[curFilename].erase(_request_window_elements[curFilename].begin() + i);
                if (elem.second.size() == 0)
                //elem.second.erase(elem.second.begin() + i);
                    toDelete.insert(elem.first);
            }
        };
    }
    for (auto const& filename: toDelete)
        _request_window_elements.erase(filename);
    
    //unordered_set<int> toDelete;
    for (auto const& elem: _eviction_window_elements){
        for(int i=0; i < elem.second.size(); i++){
            int curFilename = elem.first;
            WindowElement obj = elem.second[i];
            float size = obj.cur_values[0];
            if (_output_activation == 1) float coeff = size;
            else{
                if (size <= it_liminf_size) float coeff = 0; 
                else if (size >= it_limsup_size) float coeff = 1; 
                else float coeff = (size - it_liminf_size)/it_delta_size;
            }
            
            if ((_curRequest_from_start - obj.counter) > _time_span_evict){
                if (obj.reward != 0){  //some hits
                    if(obj.action == 0) obj.reward = + obj.reward * coeff;
                    else obj.reward = - obj.reward * coeff;
                }
                else{                  //no hits at all
                    if (obj.action == 0) obj.reward = - 1 * coeff;
                    else obj.reward = + 1 * coeff;
                }
                _cache._daily_rewards_evict.push_back(obj.reward);
                vector<float> to_add = obj.concat_with_next_values(_cache.capacity(), _cache.hit_rate());
                _evict_memory_vector.push_back(to_add);
                _eviction_window_elements[curFilename].erase(_eviction_window_elements[curFilename].begin() + i);
                if (elem.second.size() == 0)
                //elem.second.erase(elem.second.begin() + i);
                    toDelete.insert(elem.first);
                //toDelete.insert(curFilename);
            }
        }
    }
    for (auto const& filename: toDelete)
        _eviction_window_elements.erase(filename);
    //    }
};  

void env::purge(){
    unordered_set<int> toDelete;
    for (auto const& elem: _cache._stats._files){
        unordered_set<int>::iterator it;
        it = _cache._cached_files.find(elem.first);
        if ((_curRequest_from_start - elem.second._last_request) > _purge_delta && it == _cache._cached_files.end())
            toDelete.insert(elem.first);
    }
    for (auto const& filename: toDelete)
        _cache._stats._files.erase(filename);
};

void env::set_curValues(float size, float frequency, float recency, float datatype, float occupancy, float hit_rate){
    _curValues.clear();
    _curValues.push_back(size);
    _curValues.push_back(frequency);
    _curValues.push_back(recency);
    _curValues.push_back(datatype);
    _curValues.push_back(occupancy);
    _curValues.push_back(hit_rate);
}

py::array env::get_random_batch(int batch_size){
    srand(_seed);
    //int randomNumbers[batch_size];
    int randomNumber;
    //int counter = 0;
    vector<vector<float>> batch;
    //float batch[batch_size][2*6 + 1 + 1];
    if(_adding_or_evicting == 0){
        for (int index = 0; index < batch_size; index++){
            randomNumber = rand() % _add_memory_vector.size();
            batch.push_back(_add_memory_vector[randomNumber]);
            //for(int i=0; i < 2*6 + 1 + 1 ; i++)
            //    batch[counter][i] = _add_memory_vector[randomNumber][i];
            //    counter += 1;
        }
    }
    if(_adding_or_evicting == 1){
        for (int index = 0; index < batch_size; index++){
            randomNumber = rand() % _evict_memory_vector.size();
            batch.push_back(_add_memory_vector[randomNumber]);
            //for(int i=0; i < 2*6 + 1 + 1 ; i++)
            //    batch[counter][i] = _evict_memory_vector[randomNumber][i];
            //    counter += 1;
        }
    }
    py::array ret =  py::cast(batch);
    return ret;
}

bool env::check(int filename){
            unordered_map<int,FileStats>::iterator it;
            it = _cache._stats._files.find(filename);
            if (it != _cache._stats._files.end())
                return true;
            return false;
}

bool env::check_in_cache(int filename){
    unordered_set<int>::iterator it;
    it = _cache._cached_files.find(filename);
    if (it != _cache._cached_files.end())
        return true;
    return false;
}

FileStats env::get_stats(int filename){
    return _cache._stats._files[filename];
}

void env::delete_first_add_memory(){
    _add_memory_vector.erase(_add_memory_vector.begin());
}

void env::delete_first_evict_memory(){
    _evict_memory_vector.erase(_evict_memory_vector.begin());
}

int env::get_add_memory_size(){
    return _add_memory_vector.size();
}

int env::get_evict_memory_size(){
    return _evict_memory_vector.size();
}


PYBIND11_MODULE(cache_env_cpp, m) {
    // optional module docstring
    m.doc() = "pybind11 example plugin";

    // bindings to Pet class
    py::class_<FileStats>(m,"filestats") 
        .def(py::init<>())
        .def_readwrite("_size", &FileStats::_size)
        .def_readwrite("_hit", &FileStats::_hit)
        .def_readwrite("_miss", &FileStats::_miss)
        .def_readwrite("_last_request", &FileStats::_last_request)
        .def_readwrite("_datatype", &FileStats::_datatype);

    py::class_<cache>(m,"cache")
        .def(py::init<>())
        .def("capacity", &cache::capacity)
        .def("hit_rate", &cache::hit_rate)
        .def("before_request", &cache::before_request)
        .def("update_policy", &cache::update_policy)
        .def("after_request", &cache::after_request)
        .def("_get_mean_recency", &cache::_get_mean_recency)
        .def("_get_mean_frequency", &cache::_get_mean_frequency)
        .def("_get_mean_size", &cache::_get_mean_size)
        .def_readwrite("_cached_files", &cache::_cached_files)
        .def_readwrite("_cached_files_keys", &cache::_cached_files_keys)
        .def_readwrite("_daily_rewards_add", &cache::_daily_rewards_add)
        .def_readwrite("_daily_rewards_evict", &cache::_daily_rewards_evict)
        .def_readwrite("_stats", &cache::_stats)
        .def_readwrite("_size", &cache::_size)
        .def_readwrite("_max_size", &cache::_max_size)
        .def_readwrite("_hit", &cache::_hit)
        .def_readwrite("_miss", &cache::_miss)
        .def_readwrite("_daily_anomalous_CPUeff_counter", &cache::_daily_anomalous_CPUeff_counter)
        .def_readwrite("_written_data", &cache::_written_data)
        .def_readwrite("_deleted_data", &cache::_deleted_data)
        .def_readwrite("_read_data", &cache::_read_data)
        .def_readwrite("_dailyReadOnHit", &cache::_dailyReadOnHit)
        .def_readwrite("_dailyReadOnMiss", &cache::_dailyReadOnMiss)
        .def_readwrite("_daily_reward", &cache::_daily_reward)
        .def_readwrite("_CPUeff", &cache::_CPUeff)
        .def_readwrite("_h_watermark", &cache::_h_watermark)
        .def_readwrite("_l_watermark", &cache::_l_watermark);

    py::class_<env>(m, "env")
        //.def(py::init<const std::string &, int>())
        .def(py::init<int, int, std::string, std::string, std::string, int, int, int, std::string, float, int>())
        .def("purge", &env::purge)
        .def("look_for_invalidated_add_evict_accumulate", &env::look_for_invalidated_add_evict_accumulate)
        .def("update_windows_getting_eventual_rewards_accumulate", &env::update_windows_getting_eventual_rewards_accumulate)
        .def("set_curValues", &env::set_curValues)
        .def("get_random_batch", &env::get_random_batch)
        .def("check", &env::check)
        .def("check_in_cache", &env::check_in_cache)
        .def("get_stats", &env::get_stats)
        .def("get_add_memory_size", &env::get_add_memory_size)
        .def("get_evict_memory_size", &env::get_evict_memory_size)
        .def("delete_first_add_memory", &env::delete_first_add_memory)
        .def("delete_first_evict_memory", &env::delete_first_evict_memory)
        .def_readwrite("_idx_start", &env::_idx_start)
        .def_readwrite("_idx_end", &env::_idx_end)
        .def_readwrite("_cache", &env::_cache)
        .def_readwrite("_curValues", &env::_curValues)
        .def_readwrite("_adding_or_evicting", &env::_adding_or_evicting)
        .def_readwrite("_curRequest", &env::_curRequest)
        .def_readwrite("_curRequest_from_start", &env::_curRequest_from_start)
        .def_readwrite("_curDay", &env::_curDay) 
        .def_readwrite("_directory", &env::_directory)
        .def_readwrite("_df_length", &env::_df_length)
        .def_readwrite("_cached_files_index", &env::_cached_files_index); 
}


/*
void env::write_stats(){
        if (_curDay == _idx_start){
            ofstream myFile(_out_directory + "/" + _out_name);
            myFile << "date,size,hit rate,hit over miss,weighted hit rate,written data,read data,read on hit data,read on miss data,deleted data,CPU efficiency,CPU hit efficiency,CPU miss efficiency,CPU efficiency upper bound,CPU efficiency lower bound,\n";
            myFile.close();
        }
        
        ofstream myFile(_out_directory + "/" + _out_name, ios_base::app);
            myFile << [str(datetime.fromtimestamp(self.df.reqDay[0])) + ' +0000 UTC', _cache._size,
                 self._cache.hit_rate() * 100.0,
                 self._cache._hit/self._cache._miss * 100.0,
                 0,
                 self._cache._written_data,
                 self._cache._read_data,
                 self._cache._dailyReadOnHit,
                 self._cache._dailyReadOnMiss,
                 self._cache._deleted_data,
                 self._cache._CPUeff /
                 (self.df_length-self._cache._daily_anomalous_CPUeff_counter),
                 0,
                 0,
                 0,
                 0,                    
                 ])
}


void env::reset_stats(){
        _cache._hit = 0;
        _cache._miss = 0;
        _cache._written_data = 0.0;
        _cache._deleted_data = 0.0;
        _cache._read_data = 0.0;
        _cache._dailyReadOnHit = 0.0;
        _cache._dailyReadOnMiss = 0.0;
        _cache._daily_rewards_add.clear();
        _cache._daily_rewards_evict.clear();
        _cache._CPUeff = 0.0;
        _cache._daily_anomalous_CPUeff_counter = 0;
}
*/

        /*
        inline float capacity() { return (_cache._size / _cache._max_size) * 100.; };
        inline float hit_rate() { 
            if (_cache._hit != 0.)
                return _cache._hit / (_cache._hit + _cache._miss);
            return 0.;
        };
        inline FileStats before_request(int filename, bool hit, float size, int datatype, int request){
            FileStats stats;
            stats = _cache._stats.get_or_set(filename, size, datatype, request);  
            if (hit == true) 
                stats._hit += 1;
            else
                stats._miss += 1;     
            return stats;  
        };              
        inline bool update_policy(int filename, FileStats file_stats, bool hit, int action){   
            _cache._stats._files[filename] = file_stats;
            if (hit == false and action == 0){
                _cache._cached_files.insert(filename);
                return true;
            }
            else if (hit == true)
                return false;
        };
        inline void after_request(FileStats fileStats, bool hit, bool added){
            if (hit == true){
                _cache._hit += 1;
                _cache._dailyReadOnHit += fileStats._size;
            }
            else{
                _cache._miss += 1;
                _cache._dailyReadOnMiss += fileStats._size;
            }
            if (added == true){
                _cache._size += fileStats._size;
                _cache._written_data += fileStats._size;
            }
            _cache._read_data += fileStats._size;
        };
        */