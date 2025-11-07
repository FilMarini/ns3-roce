#ifndef RDMA_THROUGHPUT_MONITOR_H
#define RDMA_THROUGHPUT_MONITOR_H

#include "ns3/rdma-config-module.h"
#include "ns3/rdma-serdes.h"
//#include "ns3/periodic-event.h"
//#include "ns3/net-device-container.h"
#include "ns3/qbb-net-device.h"
#include "ns3/throughput-record.h"
#include "ns3/filesystem.h"
#include <vector>
#include <string>

namespace ns3 {

class ThroughputMonitor : public RdmaConfigModule
{
public:
    /**
     * \brief Get the type ID
     * \return the object TypeId
     */
    static TypeId GetTypeId();

    /**
     * \brief Constructor
     */
    ThroughputMonitor();

    /**
     * \brief Destructor
     */
    virtual ~ThroughputMonitor();

    /**
     * \brief Called when the module is loaded
     * \param network Reference to the RDMA network
     */
    virtual void OnModuleLoaded(RdmaNetwork& network) override;

private:
    /**
     * \brief Callback for packet transmission events
     * \param context The context string
     * \param p The transmitted packet
     * \param tx The transmitting device
     * \param rx The receiving device
     * \param tx_time Time when transmission started
     * \param rx_time Time when reception completed
     */
    void OnTxSend(
        std::string context,
        Ptr<const Packet> p,
        Ptr<NetDevice> tx,
        Ptr<NetDevice> rx,
        Time tx_time,
        Time rx_time);

    /**
     * \brief Periodic sampling callback
     * 
     * Called at each interval to calculate throughput and write records
     */
    void OnInterval();

    // Configuration attributes
    std::string m_avro_out;    ///< Output file path
    Time m_start;              ///< Start time for monitoring
    Time m_stop;               ///< Stop time for monitoring (0 = infinite)
    Time m_interval;           ///< Sampling interval

    // Runtime data
    RdmaSerializer<ThroughputRecord> m_record_writer;  ///< Avro writer
    std::vector<std::vector<uint64_t>> m_txrx_bytes;   ///< Current byte counts
    std::vector<std::vector<uint64_t>> m_last_txrx_bytes; ///< Last sample byte counts
    PeriodicEvent m_event;     ///< Periodic sampling event
    std::vector<EventId> m_events; ///< Scheduled events for cleanup
};

} // namespace ns3

#endif /* RDMA_THROUGHPUT_MONITOR_H */
