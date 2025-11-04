#include "ns3/rdma-pfc-monitor.h"
#include "ns3/rdma-network.h"
#include "ns3/qbb-net-device.h"
#include "ns3/log.h"

namespace ns3 {

NS_LOG_COMPONENT_DEFINE("PfcMonitor");
NS_OBJECT_ENSURE_REGISTERED(PfcMonitor);

TypeId PfcMonitor::GetTypeId()
{
  static TypeId tid = []() {
    static TypeId tid = TypeId("ns3::PfcMonitor");

    tid.SetParent<RdmaConfigModule>();
    tid.AddConstructor<PfcMonitor>();
    
    AddStringAttribute(tid,
      "AvroOutputFile",
      "File path where to write the Avro statistics, relatively to the config directory.",
      &PfcMonitor::m_avro_out);

    return tid;
  }();
  
  return tid;
}

PfcMonitor::PfcMonitor()
{
}

PfcMonitor::~PfcMonitor()
{
  for(EventId event : m_events) {
    event.Cancel();
  }
}

void PfcMonitor::OnModuleLoaded(RdmaNetwork& network)
{
  m_record_writer = RdmaSerializer<PfcRecord>(network.GetConfig().FindFile(m_avro_out));

  // Get all QBB network devices
  NetDeviceContainer qbb_devs = network.GetAllQbbNetDevices();

  // Connect to PFC trace for each device
  for(auto it = qbb_devs.Begin(); it != qbb_devs.End(); it++) {
    const auto dev = DynamicCast<QbbNetDevice>(*it);
    
    if(!dev) {
      continue;
    }

    auto on_pfc_state_changed = MakeLambdaCallback<uint32_t>(
      [this, dev](uint32_t state) {
        OnPfcStateChanged(dev, state == 0 ? PfcState::Resume : PfcState::Pause);
      });

    dev->TraceConnectWithoutContext("QbbPfc", on_pfc_state_changed);
  }
}

void PfcMonitor::OnPfcStateChanged(Ptr<QbbNetDevice> dev, PfcState state)
{
  PfcRecord record;
  record.time = Simulator::Now().GetSeconds();
  record.node = dev->GetNode()->GetId();
  record.dev = dev->GetIfIndex();
  record.paused = (state == PfcState::Pause);
  
  m_record_writer.write(record);
}

} // namespace ns3
