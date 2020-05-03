package removedcacheditem.removedcacheditem;

import com.google.protobuf.ByteString;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import id.id.IsId;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import removedcacheditem.input.IsInput;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ToIsRemovedCachedItemImplBaseImpl extends ToIsRemovedCachedItemGrpc.ToIsRemovedCachedItemImplBase {


    private final Logger logger;
    private final HazelcastInstance hzInstance;
    private final Map<String, ByteString> general;
    private final ILock lock;


    public ToIsRemovedCachedItemImplBaseImpl() {
        logger = Logger.getLogger(getClass().getName());
        logger.info("starting");

        logger.info("starting hazelcast");
        hzInstance = Hazelcast.newHazelcastInstance();
        logger.info("started hazelcast");

        logger.info("starting general map: hazelcast");
        general = hzInstance.getMap("general");
        logger.info("started general map: hazelcast");

        logger.info("starting general lock: hazelcast");
        lock = hzInstance.getLock("general");
        logger.info("started general lock: hazelcast");

        logger.info("started");
    }

    @Override
    public void produce(final NotRemovedCachedItem request, final StreamObserver<IsRemovedCachedItem> responseObserver) {
        try {
            final IsInput isInput;
            final IsId isId;

            if (!request.hasIsInput()) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("422"));
            }

            isInput = request.getIsInput();
            if (!isInput
                    .hasIsId()) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("422"));
            }

            isId = isInput
                    .getIsId();
            if (!isId
                    .hasIsOutput()) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("422"));
            }

            final String isStringValue = isId
                    .getIsOutput()
                    .getIsStringValue();

            if (isStringValue.isEmpty()) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("422"));
            }

            final ByteString previousValue;

            if (isInput.getIsUseLockBoolean()) {
                lock.lock();
                try {
                    previousValue = general.remove(isStringValue);
                } finally {
                    lock.unlock();
                }
            } else {
                previousValue = general.remove(isStringValue);
            }

            if (previousValue == null) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("404"));
            }

            responseObserver.onNext(IsRemovedCachedItem.newBuilder()
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.log(Level.SEVERE,
                    e.getMessage(),
                    e);
            responseObserver.onError(e);
        }
    }
}
