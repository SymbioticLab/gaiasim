package net.floodlightcontroller.flowinstaller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.OFFlowMod;
import org.projectfloodlight.openflow.protocol.OFFlowAdd;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.action.OFActionOutput;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.protocol.instruction.OFInstruction;
import org.projectfloodlight.openflow.protocol.instruction.OFInstructions;
import org.projectfloodlight.openflow.protocol.instruction.OFInstructionApplyActions;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.IpProtocol;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TransportPort;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;


public class TrySet implements Runnable {
    protected IOFSwitchService switchService_;

    public TrySet(IOFSwitchService switchService) {
        switchService_ = switchService;
    }

    public void run() {
        IOFSwitch sw = null;
        try {
            while ((sw = switchService_.getSwitch(DatapathId.of(1))) == null) {
                Thread.sleep(1000);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        Match m = sw.getOFFactory().buildMatch()
            .setExact(MatchField.ETH_TYPE, EthType.IPv4)
            .setExact(MatchField.IPV4_SRC, IPv4Address.of("10.0.0.2"))
            .setExact(MatchField.IPV4_DST, IPv4Address.of("10.0.0.3"))
            .setExact(MatchField.IP_PROTO, IpProtocol.TCP)
            .setExact(MatchField.TCP_SRC, TransportPort.of(12345))
            .setExact(MatchField.TCP_DST, TransportPort.of(54321))
            .build();

        OFActionOutput.Builder aob = sw.getOFFactory().actions().buildOutput();
        aob.setPort(OFPort.of(2));
        List<OFAction> actions = new ArrayList<OFAction>();
        actions.add(aob.build());

        OFInstructions ib = sw.getOFFactory().instructions();
        OFInstructionApplyActions applyActions = ib.buildApplyActions()
            .setActions(actions)
            .build();
        List<OFInstruction> instructions = new ArrayList<OFInstruction>();
        instructions.add(applyActions);

        OFFlowAdd fm = sw.getOFFactory().buildFlowAdd()
            .setMatch(m)
            .setPriority(3)
            .setOutPort(OFPort.of(2))
            .setInstructions(instructions)
            .build();

        sw.write(fm);
    }
}
