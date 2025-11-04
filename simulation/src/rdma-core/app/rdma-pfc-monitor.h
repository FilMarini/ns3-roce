#pragma once

#include "ns3/pfc-record.h"
#include "ns3/filesystem.h"
#include "ns3/rdma-serdes.h"
#include "ns3/net-device-container.h"
#include "ns3/qbb-net-device.h"
#include "ns3/rdma-config-module.h"

namespace ns3 {

  class PfcMonitor final : public RdmaConfigModule
  {
  public:
    static TypeId GetTypeId();
  
    PfcMonitor();
    ~PfcMonitor();

    void OnModuleLoaded(RdmaNetwork& network) override;

  private:

  enum PfcState {
    Pause,
    Resume
  };

    void OnPfcStateChanged(Ptr<QbbNetDevice> dev, PfcState state);

    std::string m_avro_out;
    RdmaSerializer<PfcRecord> m_record_writer;
    std::vector<EventId> m_events;
  };

} // namespace ns33
