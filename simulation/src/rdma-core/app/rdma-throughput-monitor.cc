#include "ns3/rdma-throughput-monitor.h"
#include "ns3/rdma-network.h"
#include "ns3/rdma-reflection-helper.h"
#include "ns3/config.h"
#include "ns3/log.h"
#include "ns3/simulator.h"

namespace ns3 {

NS_LOG_COMPONENT_DEFINE("ThroughputMonitor");
NS_OBJECT_ENSURE_REGISTERED(ThroughputMonitor);

TypeId ThroughputMonitor::GetTypeId()
{
    static TypeId tid = []() {
        static TypeId tid = TypeId("ns3::ThroughputMonitor");

        tid.SetParent<RdmaConfigModule>();
        tid.AddConstructor<ThroughputMonitor>();

        AddStringAttribute(tid,
            "AvroOutputFile",
            "File path where to write the Avro statistics, relatively to the config directory.",
            &ThroughputMonitor::m_avro_out);

        AddTimeAttribute(tid,
            "StartTime",
            "Time when to start gathering statistics.",
            &ThroughputMonitor::m_start);

        AddTimeAttribute(tid,
            "StopTime",
            "Time when to stop gathering statistics. Set to zero for infinite.",
            &ThroughputMonitor::m_stop);

        AddTimeAttribute(tid,
            "IntervalTime",
            "Interval between two gathering of statistics.",
            &ThroughputMonitor::m_interval);

        return tid;
    }();

    return tid;
}

ThroughputMonitor::ThroughputMonitor()
{
    NS_LOG_FUNCTION(this);
}

ThroughputMonitor::~ThroughputMonitor()
{
    NS_LOG_FUNCTION(this);
    
    // Cancel any pending events
    for (EventId event : m_events) {
        event.Cancel();
    }
}

void ThroughputMonitor::OnModuleLoaded(RdmaNetwork& network)
{
    NS_LOG_FUNCTION(this);

    // Initialize the Avro writer
    m_record_writer = RdmaSerializer<ThroughputRecord>(
        network.GetConfig().FindFile(m_avro_out));

    // Get all QBB devices
    NetDeviceContainer devs = network.GetAllQbbNetDevices();

    // Find the maximum node ID to size our matrices
    uint32_t max_node_id = 0;
    for (uint32_t i = 0; i < devs.GetN(); ++i) {
        uint32_t node_id = devs.Get(i)->GetNode()->GetId();
        if (node_id > max_node_id) {
            max_node_id = node_id;
        }
    }

    // Size the matrices to accommodate the highest node ID
    const size_t matrix_size = max_node_id + 1;
    
    m_txrx_bytes.resize(matrix_size);
    m_last_txrx_bytes.resize(matrix_size);
    
    for (auto& v : m_txrx_bytes) {
        v.resize(matrix_size, 0);
    }
    for (auto& v : m_last_txrx_bytes) {
        v.resize(matrix_size, 0);
    }

    NS_LOG_INFO("ThroughputMonitor: Initialized matrix of size " << matrix_size 
                << "x" << matrix_size);

    // Connect to the packet transmission trace
    const char* const callback_path = "/ChannelList/*/TxRxPointToPoint";
    auto callback = MakeCallback(&ThroughputMonitor::OnTxSend, this);
    bool connected = Config::ConnectFailSafe(callback_path, callback);
    
    if (!connected) {
        NS_LOG_WARN("ThroughputMonitor: Failed to connect to trace source");
    }

    // Setup the periodic event
    m_event.SetTask([this]() {
        OnInterval();
    });

    // Validate and set interval
    if (m_interval.IsZero()) {
        const Time fallback_interval = Seconds(0.01);
        NS_LOG_WARN("The interval cannot be zero. Setting it to " << fallback_interval);
        m_interval = fallback_interval;
    }

    m_event.SetInterval(m_interval);

    // Schedule start
    m_events.push_back(Simulator::Schedule(m_start, [this]() {
        NS_LOG_INFO("ThroughputMonitor: Starting monitoring at " 
                    << Simulator::Now().GetSeconds() << "s");
        m_event.Resume();
    }));

    // Schedule stop (if specified)
    if (!m_stop.IsZero()) {
        m_events.push_back(Simulator::Schedule(m_stop, [this]() {
            NS_LOG_INFO("ThroughputMonitor: Stopping monitoring at " 
                        << Simulator::Now().GetSeconds() << "s");
            m_event.Pause();
        }));
    }

    NS_LOG_INFO("ThroughputMonitor: Module loaded successfully");
}

void ThroughputMonitor::OnTxSend(
    std::string context,
    Ptr<const Packet> p,
    Ptr<NetDevice> tx,
    Ptr<NetDevice> rx,
    Time tx_time,
    Time rx_time)
{
    // Update the byte counter for this source-destination pair
    uint32_t src_id = tx->GetNode()->GetId();
    uint32_t dst_id = rx->GetNode()->GetId();
    
    m_txrx_bytes[src_id][dst_id] += p->GetSize();
}

void ThroughputMonitor::OnInterval()
{
    NS_LOG_FUNCTION(this);

    const Time now = Simulator::Now();
    const double interval_sec = m_interval.GetSeconds();

    // Iterate through all source-destination pairs
    for (size_t tx_i = 0; tx_i < m_txrx_bytes.size(); tx_i++) {
        for (size_t rx_i = 0; rx_i < m_txrx_bytes[tx_i].size(); rx_i++) {
            
            const uint64_t bytes_now = m_txrx_bytes[tx_i][rx_i];
            const uint64_t bytes_last = m_last_txrx_bytes[tx_i][rx_i];
            const uint64_t bytes_delta = bytes_now - bytes_last;

            // Only record if there was traffic in this interval
            if (bytes_delta > 0) {
                // Calculate throughput in Gbps
                double throughput_gbps = (bytes_delta * 8.0) / (interval_sec * 1e9);

                // Create and write the record
                ThroughputRecord record;
                record.time = now.GetSeconds();
                record.src = tx_i;
                record.dst = rx_i;
                record.bytes_delta = bytes_delta;
                record.throughput_gbps = throughput_gbps;

                m_record_writer.write(record);

                NS_LOG_DEBUG("ThroughputMonitor: Node " << tx_i << " -> " << rx_i 
                            << " throughput: " << throughput_gbps << " Gbps");
            }

            // Update last sample
            m_last_txrx_bytes[tx_i][rx_i] = bytes_now;
        }
    }
}

} // namespace ns3
