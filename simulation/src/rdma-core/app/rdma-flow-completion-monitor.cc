#include "ns3/rdma-flow-completion-monitor.h"
#include "ns3/rdma-network.h"
#include "ns3/rdma-flow-scheduler.h"
#include "ns3/rdma-flow.h"
#include "ns3/rdma-flow-unicast.h"

namespace ns3 {

NS_LOG_COMPONENT_DEFINE("FlowCompletionMonitor");
NS_OBJECT_ENSURE_REGISTERED(FlowCompletionMonitor);

TypeId FlowCompletionMonitor::GetTypeId()
{
  static TypeId tid = []() {
    static TypeId tid = TypeId("ns3::FlowCompletionMonitor");
    
    tid.SetParent<RdmaConfigModule>();
    tid.AddConstructor<FlowCompletionMonitor>();
    
    AddStringAttribute(tid,
      "AvroOutputFile",
      "File path where to write flow completion statistics.",
      &FlowCompletionMonitor::m_avro_out);

    return tid;
  }();
  
  return tid;
}

void FlowCompletionMonitor::OnModuleLoaded(RdmaNetwork& network)
{
  m_writer = RdmaSerializer<FlowCompletionRecord>(
    network.GetConfig().FindFile(m_avro_out)
  );
  
  // Register callback with flow scheduler
  network.GetFlowScheduler().AddFlowCompletionCallback(
    [this](Ptr<RdmaFlow> flow) {
      OnFlowComplete(flow);
    }
  );
  
  NS_LOG_INFO("FlowCompletionMonitor loaded");
}

void FlowCompletionMonitor::OnFlowComplete(Ptr<RdmaFlow> flow)
{
  FlowCompletionRecord record;
  
  record.flow_id = flow->GetId();
  record.start_time = flow->GetStartTime().GetSeconds();
  record.completion_time = Simulator::Now().GetSeconds();
  record.duration = record.completion_time - record.start_time;
  
  // Initialize with defaults
  record.src = -1;
  record.dst = -1;
  record.bytes_requested = 0;
  record.bytes_sent = 0;
  record.priority = 0;
  record.is_reliable = false;
  
  // Get flow-specific info if it's a unicast flow
  if(Ptr<RdmaFlowUnicast> unicast = DynamicCast<RdmaFlowUnicast>(flow)) {
    // You'll need to add public getter methods to RdmaFlowUnicast class:
    // uint32_t GetSourceNode() const { return m_snode; }
    // uint32_t GetDestinationNode() const { return m_dnode; }
    // uint64_t GetWriteByteAmount() const { return m_bytes_to_write; }
    // uint16_t GetPriority() const { return m_priority; }
    // bool IsReliable() const { return m_reliable; }
    
    // For now, these will cause compilation errors until you add the getters:
    record.src = unicast->GetSourceNode();
    record.dst = unicast->GetDestinationNode();
    record.bytes_requested = unicast->GetWriteByteAmount();
    record.priority = unicast->GetPriority();
    record.is_reliable = unicast->IsReliable();
    record.bytes_sent = record.bytes_requested; // Simplified for now
  }
  
  m_writer.write(record);
  
  NS_LOG_INFO("Flow " << record.flow_id 
              << " completed at " << record.completion_time << "s"
              << " (duration: " << record.duration << "s)");
}

FlowCompletionMonitor::~FlowCompletionMonitor()
{
  // Writer auto-closes
}

} // namespace ns3
