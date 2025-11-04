#ifndef RDMA_FLOW_COMPLETION_MONITOR_H
#define RDMA_FLOW_COMPLETION_MONITOR_H

#include "ns3/rdma-serdes.h"
#include "ns3/rdma-config-module.h"
#include "ns3/rdma-reflection-helper.h"
#include "ns3/flow-record.h"  // Use the generated file
#include "ns3/ptr.h"

namespace ns3 {

  // Forward declaration
  class RdmaFlow;

  class FlowCompletionMonitor : public RdmaConfigModule
  {
  public:
    static TypeId GetTypeId();
  
    virtual void OnModuleLoaded(RdmaNetwork& network) override;
    virtual ~FlowCompletionMonitor();

  private:
    void OnFlowComplete(Ptr<RdmaFlow> flow);
  
    std::string m_avro_out;
    RdmaSerializer<FlowCompletionRecord> m_writer;
  };

} // namespace ns3

#endif
