package cn.xhjava.flink.cdc.stream.oracle;

import oracle.streams.*;

public class LcrEventHandler implements XStreamLCRCallbackHandler {

    @Override
    public void processLCR(LCR lcr) throws StreamsException {

        if (lcr instanceof RowLCR) {
            RowLCR rowLCR = (RowLCR) lcr;
            System.out.println("CommandType: " + rowLCR.getCommandType());
            ColumnValue[] oldValues = rowLCR.getOldValues();
            ColumnValue[] newValues = rowLCR.getNewValues();
        } else if (lcr instanceof DDLLCR) {
//                dispatchSchemaChangeEvent((DDLLCR) lcr);
            System.out.println("process DDLLCR");
        }
    }

    @Override
    public void processChunk(ChunkColumnValue chunkColumnValue) throws StreamsException {
        System.out.println("processChunk");
    }

    @Override
    public LCR createLCR() throws StreamsException {
        System.out.println("createLCR");
        return null;
    }

    @Override
    public ChunkColumnValue createChunk() throws StreamsException {
        System.out.println("createChunk");
        return null;
    }
}
