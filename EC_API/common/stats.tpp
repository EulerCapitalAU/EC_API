/* ------------ OHLCVStat ------------*/
// OHLCVStat: O(1) for update, O(N) at worst in evict
template <typename ContainerT>
OHLCVStat<ContainerT>::OHLCVStat(ContainerT& container, double tick_size):
        container_{&container},
        tick_size_{tick_size},
        ohlcv_snapshot{} {}
        
template <typename ContainerT>
void OHLCVStat<ContainerT>::update(const TradeTick& t) {
    if (t.price >= ohlcv_snapshot.high) {ohlcv_snapshot.high = t.price;}
    if (t.price <= ohlcv_snapshot.low) {ohlcv_snapshot.low = t.price;}
    
    ohlcv_snapshot.open = container_->front().price;
    ohlcv_snapshot.close = container_->back().price;
    ohlcv_snapshot.volume += t.volume;
}

template <typename ContainerT>
void OHLCVStat<ContainerT>::evict(const TradeTick& t) {
    ohlcv_snapshot.volume -= t.volume;
    
    if (container_->empty()) {//guard gainst empty window
        ohlcv_snapshot = OHLCVSnapshot{};
        return;
    }

    if (std::abs(t.price - ohlcv_snapshot.high) <= 0.5*tick_size_) { 
        auto it = std::max_element(container_->begin(), container_->end(), [](const TradeTick& a, const TradeTick& b) {return a.price < b.price;}); 
        ohlcv_snapshot.high = it->price;
        }
    if (std::abs(t.price - ohlcv_snapshot.low) <= 0.5*tick_size_) {
        auto it = std::min_element(container_->begin(), container_->end(),[](const TradeTick& a, const TradeTick& b) {return a.price < b.price;});
        ohlcv_snapshot.low = it->price;
        }

}

template <typename ContainerT>
OHLCVSnapshot OHLCVStat<ContainerT>::get_snapshot() const {return ohlcv_snapshot;}

template <typename ContainerT>
py::object OHLCVStat<ContainerT>::to_py_tuple() const {
    OHLCVSnapshot ohlcv_snapshot = get_snapshot();
    return py::make_tuple(
        ohlcv_snapshot.open, 
        ohlcv_snapshot.high,
        ohlcv_snapshot.low,
        ohlcv_snapshot.close,
        ohlcv_snapshot.volume);
    }

/* ------------ MomentStat ------------*/
// MomentStat: O(1) on both update and evict
template <typename ContainerT>
MomentStat<ContainerT>::MomentStat(ContainerT& container):
    container_{&container}, 
    sum_p{}, sum_p_2{}, sum_p_3{}, sum_p_4{}, 
    count{}, 
    moment_snapshot{} {
      for (const TradeTick& t : *container_)
          update(t);
    }
    
template <typename ContainerT>   
void MomentStat<ContainerT>::update(const TradeTick& t) {
    double p = t.price;
    sum_p += p;
    sum_p_2 += std::pow(p,2);
    sum_p_3 += std::pow(p,3);
    sum_p_4 += std::pow(p,4);
    count++;
    
    double mean_ = sum_p / count;
    double variance_ = (sum_p_2/count) - std::pow(mean_, 2); 
    
    moment_snapshot.mean = mean_;
    moment_snapshot.variance = variance_;
    moment_snapshot.skewness = (((sum_p_3/count) - 3*mean_*(sum_p_2 / count) + 2* std::pow(mean_, 3)) / pow(variance_, 1.5)); 
    moment_snapshot.kurtosis = (((sum_p_4 -4 * mean_ * sum_p_3 + 6 * sum_p_2 * std::pow(mean_,2))/count) - 3* std::pow(mean_,4)) /  pow(variance_, 2);
}

template <typename ContainerT>
void MomentStat<ContainerT>::evict(const TradeTick& t) {
    double p = t.price;
    sum_p -= p;
    sum_p_2 -= std::pow(p, 2);
    sum_p_3 -= std::pow(p, 3);
    sum_p_4 -= std::pow(p, 4);
    count--;
            
    double mean_ = sum_p / count;
    double variance_ = (sum_p_2/count) - std::pow(mean_, 2); 
    
    moment_snapshot.mean = mean_;
    moment_snapshot.variance = variance_;
    moment_snapshot.skewness = (((sum_p_3/count) - 3*mean_*(sum_p_2 / count) + 2* std::pow(mean_, 3)) / pow(variance_, 1.5)); 
    moment_snapshot.kurtosis = (((sum_p_4 -4 * mean_ * sum_p_3 + 6 * sum_p_2 * std::pow(mean_,2))/count) - 3* std::pow(mean_,4)) /  pow(variance_, 2);
}

template <typename ContainerT>
MomentSnapshot MomentStat<ContainerT>::get_snapshot() const {return moment_snapshot;}

template <typename ContainerT>
py::object MomentStat<ContainerT>::to_py_tuple() const {
    MomentSnapshot moment_snapshot = get_snapshot();
    return py::make_tuple(
        moment_snapshot.mean, 
        moment_snapshot.variance, 
        moment_snapshot.skewness, 
        moment_snapshot.kurtosis
    );
}

/* ------------ VWAPStat ------------*/
// VWAPStat: O(1) on both update and evict
template <typename ContainerT>
VWAPStat<ContainerT>::VWAPStat(ContainerT& container): 
    container_{&container},
    sum_pv{}, 
    sum_v{},
    vwap_snapshot{} {
     for (const TradeTick& t : *container_)
         update(t);
    }
    
template <typename ContainerT>
void VWAPStat<ContainerT>::update(const TradeTick& t) {
  sum_pv += t.price * t.volume;
  sum_v  += t.volume;
  vwap_snapshot.vwap =  (sum_v > 0.0) ? sum_pv / sum_v
                        : std::numeric_limits<double>::quiet_NaN();
}

template <typename ContainerT>
void VWAPStat<ContainerT>::evict(const TradeTick& t) {
  sum_pv -= t.price * t.volume;
  sum_v  -= t.volume;         
  vwap_snapshot.vwap = sum_pv / sum_v;
}

template <typename ContainerT>
VWAPSnapshot VWAPStat<ContainerT>::get_snapshot() const {return vwap_snapshot;}

template <typename ContainerT>
py::object VWAPStat<ContainerT>::to_py_tuple() const {
    VWAPSnapshot vwap_snapshot = get_snapshot();
    return py::make_tuple(
        vwap_snapshot.vwap
    );
}

/* ------------ MedianStat ------------*/
template <typename ContainerT>
MedianStat<ContainerT>::MedianStat(ContainerT& container):
    container_{&container},   
    median{},  
    maxHeap{},
    minHeap{},
    median_snapshot{} {
        for (const TradeTick& t : *container_)
            update(t);
    }

template <typename ContainerT>
void MedianStat<ContainerT>::update(const TradeTick& t) {}

template <typename ContainerT>
void MedianStat<ContainerT>::evict(const TradeTick& t) {}

template <typename ContainerT>
MedianSnapshot MedianStat<ContainerT>::get_snapshot() const {return median_snapshot;}

template <typename ContainerT>
py::object MedianStat<ContainerT>::to_py_tuple() const {
    MedianSnapshot median_snapshot = get_snapshot();
    return py::make_tuple(
        median_snapshot.median
    );
}

/* ============== Stat setup ================ */
template <typename C>
void init_stats(
    StatConfig& config, 
    C& tick_container,
    std::array<StatBase*, kStatTypeCount>& stats_array, 
    std::vector<std::unique_ptr<StatBase>>& active_,
    double tick_size
    ) {
    
    if (config.cal_ohlcv) {
        auto p_ = std::make_unique<OHLCVStat<C>>(tick_container, tick_size);
        stats_array[static_cast<size_t>(StatType::OHLCV)] = p_.get(); // hold unique pointers
        active_.push_back(std::move(p_));
        }
    
    if (config.cal_moment) {
        auto p_ = std::make_unique<MomentStat<C>>(tick_container);
        stats_array[static_cast<size_t>(StatType::MOMENT)] = p_.get();
        active_.push_back(std::move(p_));
        }
    
    if (config.cal_vwap) {
        auto p_ = std::make_unique<VWAPStat<C>>(tick_container);
        stats_array[static_cast<size_t>(StatType::VWAP)] = p_.get();
        active_.push_back(std::move(p_));
        }
    
    if (config.cal_median) {
        auto p_ = std::make_unique<MedianStat<C>>(tick_container);
        stats_array[static_cast<size_t>(StatType::MEDIAN)] = p_.get();
        active_.push_back(std::move(p_));
        }
    }
