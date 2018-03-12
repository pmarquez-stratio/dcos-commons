package com.mesosphere.sdk.queues.state;

import com.mesosphere.sdk.offer.LoggingUtils;
import com.mesosphere.sdk.queues.generator.Generator;
import com.mesosphere.sdk.scheduler.AbstractScheduler;
import com.mesosphere.sdk.state.StateStore;
import com.mesosphere.sdk.state.StateStoreException;
import com.mesosphere.sdk.storage.Persister;
import com.mesosphere.sdk.storage.PersisterException;
import com.mesosphere.sdk.storage.PersisterUtils;
import com.mesosphere.sdk.storage.StorageError;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;

/**
 * This class maintains persistent storage of active runs. The underlying data is colocated in the service's StateStore
 * in order to ensure that it's automatically erased when the service itself is torn down.
 *
 * The original run data is stored as-is, allowing any improvements to spec parsing to be inherited by existing jobs.
 *
 * Specs are stored as follows:
 * <br>/
 * <br>&nbsp; Specs/
 * <br>&nbsp; &nbsp; spec-id-1/
 * <br>&nbsp; &nbsp; &nbsp; Type
 * <br>&nbsp; &nbsp; &nbsp; Data
 * <br>&nbsp; &nbsp; spec-id-2/
 * <br>&nbsp; &nbsp; &nbsp; Type
 * <br>&nbsp; &nbsp; &nbsp; Data
 */
public class SpecStore {

    private static final Logger LOGGER = LoggingUtils.getLogger(SpecStore.class);

    /**
     * The charset used when serializing/deserializing id and type strings.
     */
    private static final Charset STORAGE_CHARSET = StandardCharsets.UTF_8;

    /**
     * The property name to use in each service's StateStore, to map the service back to its associated Spec.
     */
    private static final String SERVICE_STATE_STORE_ID_PROPERTY = "spec-id";

    /**
     * The folder under the queue root where specs are stored.
     */
    private static final String SPECS_ROOT_NAME = "Specs";

    /**
     * The type of the spec. This is a label like "sdk-yaml" or "spark". The Queue should have a {@code RunGenerator}
     * associated with each Type listed in the SpecStore.
     */
    private static final String SPEC_TYPE_PATH_NAME = "Type";

    /**
     * The original spec data as submitted by the user. We keep the original submission data mainly in order to support
     * user debugging, but also to allow existing jobs to inherit refinements in the {@code RunGenerator}s without
     * requiring resubmission by the user.
     */
    private static final String SPEC_DATA_PATH_NAME = "Data";

    private final Persister persister;

    public SpecStore(Persister persister) {
        this.persister = persister;
    }

    /**
     * Stores the provided spec data against the provided service as represented by a {@link StateStore}. The spec data
     * will be stored in the SpecStore (reusing an existing ID if possible), while the service's {@link StateStore} will
     * get a {@code spec-id} property pointing that service to its associated Spec. This property will automatically be
     * deleted when the service is removed or uninstalled.
     *
     * @param runStateStore the state store of the service to be launched against this spec
     * @param data the spec data as submitted by the user
     * @param type the type of data, or an empty optional if no type was provided (valid iff there's just one generator)
     * @throws PersisterException if there's an error accessing the underlying storage
     */
    public void store(StateStore runStateStore, byte[] data, String type) throws PersisterException {
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }
        if (Strings.isEmpty(type)) {
            throw new IllegalArgumentException("Type cannot be null or empty");
        }

        // Write the data to the spec store, or reuse an existing id if available:
        final String specId = toSpecId(type, data);
        final String specTypePath = getSpecTypePath(specId);
        final String specDataPath = getSpecDataPath(specId);

        byte[] typeBytes = type.getBytes(STORAGE_CHARSET);

        // Check if a spec with this id already exists in the spec store.
        Map<String, byte[]> specEntries = persister.getMany(Arrays.asList(specTypePath, specDataPath));
        byte[] storedType = specEntries.get(specTypePath);
        byte[] storedData = specEntries.get(specDataPath);
        if (storedType == null && storedData == null) {
            // Spec doesn't exist yet. Store and proceed.
            LOGGER.info("Storing new {} byte {} spec with id {}", data.length, type, specId);
            specEntries.put(specDataPath, data);
            specEntries.put(specTypePath, typeBytes);
            persister.setMany(specEntries);
        } else {
            // Spec already exists. As a sanity check, verify the stored type/data exactly match before continuing.
            // Any mismatches imply (in decreasing likelihood):
            // - A bug in the queues service
            // - Someone tampered with the persister's storage
            // - A sha256 collision(!)
            LOGGER.info("Verifying existing {} byte {} spec with id {}", data.length, type, specId);
            if (!Arrays.equals(typeBytes, storedType) || !Arrays.equals(data, storedData)) {
                LOGGER.error("Mismatch between stored data and submitted data for Spec {}. Bad spec data?:", specId);
                LOGGER.error(getEntryDescription("SpecStore", storedType, storedData));
                LOGGER.error(getEntryDescription("Submission", typeBytes, data));
                throw new PersisterException(
                        StorageError.Reason.LOGIC_ERROR,
                        String.format("Data mismatch between existing data and submitted data for specId %s", specId));
            }
        }

        // Write the spec id to the service. This is used by recover() to map active runs back to their job specs.
        runStateStore.storeProperty(SERVICE_STATE_STORE_ID_PROPERTY, specId.getBytes(STORAGE_CHARSET));
    }

    /**
     * Returns the spec id of the provided active run, or an empty {@link Optional} if none was found
     *
     * @param scheduler the scheduler to fetch against
     * @return the spec id, or an empty optional
     * @throws StateStoreException if there was an underlying storage problem
     */
    public Optional<String> getSpecId(AbstractScheduler scheduler) throws StateStoreException{
        try {
            String specId = new String(
                    scheduler.getStateStore().fetchProperty(SERVICE_STATE_STORE_ID_PROPERTY),
                    STORAGE_CHARSET);
            return Optional.of(specId);
        } catch (StateStoreException e) {
            if (e.getReason() == StorageError.Reason.NOT_FOUND) {
                return Optional.empty();
            } else {
                throw e;
            }
        }
    }

    /**
     * Recovers all available spec data from previous runs, and regenerates those runs. This should be invoked by the
     * Queue on startup to resume any previously running services.
     *
     * TODO(nickbp): This would be a good spot to prune unused/dangling specs from the SpecStore.
     *
     * @param generators the configured generators used to recover previous runs
     * @return a collection of successfully generated runs, or an empty list if no runs were found
     * @throws PersisterException if there's a problem with the underlying data
     * @throws Exception if any of the generators failed to re-generate a run
     */
    public Collection<AbstractScheduler> recover(Map<String, Generator> generators) throws Exception {
        // Scrape the service data for any spec ids:
        boolean anyMalformed = false;
        Map<String, String> serviceNameToSpecId = new HashMap<>();
        for (String serviceNamespace : PersisterUtils.fetchServiceNamespaces(persister)) {
            try {
                byte[] specIdBytes =
                        new StateStore(persister, serviceNamespace).fetchProperty(SERVICE_STATE_STORE_ID_PROPERTY);
                serviceNameToSpecId.put(serviceNamespace, new String(specIdBytes, STORAGE_CHARSET));
            } catch (StateStoreException e) {
                // The spec id should always be present. We write it before launching the service and leave it as-is
                // until the service is uninstalled. Log an error and continue. We wait until the end to throw an
                // exception, so that all problems are logged.
                anyMalformed = true;
                LOGGER.error(String.format(
                        "Failed to retrieve expected property=%s for service=%s. Corrupt service data?",
                        SERVICE_STATE_STORE_ID_PROPERTY, serviceNamespace), e);
            }
        }
        LOGGER.info("Found services: {}", serviceNameToSpecId);
        if (anyMalformed) {
            throw new PersisterException(
                    StorageError.Reason.LOGIC_ERROR,
                    "One or more services have invalid or missing Spec id properties. See errors above.");
        }

        // Retrieve all the specs listed in the per-service spec ids.
        Map<String, SpecInfo> specIdToSpec = new HashMap<>();
        for (String specId : serviceNameToSpecId.values().stream().distinct().collect(Collectors.toList())) {
            final String specTypePath = getSpecTypePath(specId);
            final String specDataPath = getSpecDataPath(specId);
            Map<String, byte[]> specEntries = persister.getMany(Arrays.asList(specTypePath, specDataPath));
            SpecInfo specInfo = new SpecInfo(specEntries.get(specTypePath), specEntries.get(specDataPath));
            if (specInfo.data == null || specInfo.type == null) {
                // The spec data and/or type are missing. We wait until the end to throw an exception, so that all
                // problems are logged.
                anyMalformed = true;
                LOGGER.error("Missing spec data or type for spec id '{}' used by services: {}",
                        specId,
                        serviceNameToSpecId.entrySet().stream()
                                .filter(e -> e.getValue().equals(specId))
                                .map(e -> e.getKey())
                                .collect(Collectors.toSet()));
            } else {
                specIdToSpec.put(specId, specInfo);
            }
        }
        LOGGER.info("Retrieved specs: {}", specIdToSpec.keySet());
        if (anyMalformed) {
            throw new PersisterException(
                    StorageError.Reason.LOGIC_ERROR,
                    "One or more expected Specs are malformed or missing. See errors above.");
        }

        // With all the specs retrieved, invoke the matching generator for each.
        Collection<AbstractScheduler> services = new ArrayList<>();
        for (Map.Entry<String, String> serviceNameSpecId : serviceNameToSpecId.entrySet()) {
            SpecInfo spec = specIdToSpec.get(serviceNameSpecId.getValue());
            if (spec == null) {
                // Shouldn't happen. We just fetched this!
                throw new IllegalStateException(String.format(
                        "Missing spec mapping for service %s, specid %s",
                        serviceNameSpecId.getKey(), serviceNameSpecId.getValue()));
            }

            String typeStr = new String(spec.type, STORAGE_CHARSET);
            final Generator matchingGenerator = generators.get(typeStr);
            if (matchingGenerator == null) {
                anyMalformed = true;
                // Generators don't cover the specified spec type.
                LOGGER.error("Missing generator with type={} for spec {} (service {}). Generator types are: {}",
                        typeStr, serviceNameSpecId.getValue(), serviceNameSpecId.getKey(), generators.keySet());
                continue;
            }
            services.add(matchingGenerator.generate(spec.data));
        }
        LOGGER.info("Recovered services: {}",
                services.stream()
                        .map(sched -> sched.getServiceSpec().getName())
                        .collect(Collectors.toList()));
        if (anyMalformed) {
            throw new PersisterException(
                    StorageError.Reason.LOGIC_ERROR, "One or more expected Generators is missing. See errors above.");
        }

        return services;
    }

    /**
     * Returns a user-facing description of the provided spec type/data for use in logging.
     */
    private static String getEntryDescription(String label, byte[] type, byte[] data) {
        return String.format("%s: type (%d bytes): '%s', data (%d bytes):%n%s",
                label,
                type == null ? 0 : type.length,
                type == null ? "<null>" : new String(type, STORAGE_CHARSET),
                data == null ? 0 : data.length,
                data == null ? "<null>" : new String(data, STORAGE_CHARSET));
    }

    /**
     * @return Specs/[specId]/Type
     */
    private static String getSpecTypePath(String specId) {
        return PersisterUtils.join(getSpecPath(specId), SPEC_TYPE_PATH_NAME);
    }

    /**
     * @return Specs/[specId]/Data
     */
    private static String getSpecDataPath(String specId) {
        return PersisterUtils.join(getSpecPath(specId), SPEC_DATA_PATH_NAME);
    }

    /**
     * @return Specs/[specId]
     */
    private static String getSpecPath(String specId) {
        return PersisterUtils.join(SPECS_ROOT_NAME, specId);
    }

    /**
     * Given the provided spec type and spec content, returns an id string which uniquely identifies the spec.
     *
     * @param type the spec type, which is included as part of the spec id
     * @param data the spec content, which is used to generate a unique id
     * @return a unique id for addressing specs with identical type + content
     */
    private static String toSpecId(String type, byte[] data) {
        return String.format("%s-%s", type, DigestUtils.sha256Hex(data));
    }

    /**
     * Utility class for pairing spec content with the spec type.
     */
    private static class SpecInfo {
        private final byte[] type;
        private final byte[] data;

        private SpecInfo(byte[] type, byte[] data) {
            this.type = type;
            this.data = data;
        }
    }
}
