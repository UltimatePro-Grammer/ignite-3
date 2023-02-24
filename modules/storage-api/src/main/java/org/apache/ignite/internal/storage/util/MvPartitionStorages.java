/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.util;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.util.StorageOperation.AbortRebalanceStorageOperation;
import org.apache.ignite.internal.storage.util.StorageOperation.CleanupStorageOperation;
import org.apache.ignite.internal.storage.util.StorageOperation.CreateStorageOperation;
import org.apache.ignite.internal.storage.util.StorageOperation.DestroyStorageOperation;
import org.apache.ignite.internal.storage.util.StorageOperation.FinishRebalanceStorageOperation;
import org.apache.ignite.internal.storage.util.StorageOperation.StartRebalanceStorageOperation;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Class for storing stores and performing operations on them.
 */
public class MvPartitionStorages<T extends MvPartitionStorage> {
    private final TableView tableView;

    private final AtomicReferenceArray<T> storageByPartitionId;

    private final ConcurrentMap<Integer, StorageOperation> operationByPartitionId = new ConcurrentHashMap<>();

    private final ConcurrentMap<Integer, CompletableFuture<Void>> rebalaceFutureByPartitionId = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param tableView Table configuration.
     */
    public MvPartitionStorages(TableView tableView) {
        this.tableView = tableView;

        storageByPartitionId = new AtomicReferenceArray<>(tableView.partitions());
    }

    /**
     * Returns the multi-versioned partition storage, {@code null} if the storage does not exist (not created or destroyed).
     *
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     */
    public @Nullable T get(int partitionId) {
        checkPartitionId(partitionId);

        return storageByPartitionId.get(partitionId);
    }

    /**
     * Creates and adds a new multi-versioned partition storage, if the storage is in the process of being destroyed, it will be recreated
     * after the destruction.
     *
     * @param partitionId Partition ID.
     * @param createStorageFunction Storage creation function, the argument is the partition ID.
     * @return Future of creating a multi-versioned partition storage.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageException If the storage already exists or another operation is already in progress.
     * @throws StorageException If the creation of the storage after its destruction is already planned.
     */
    public CompletableFuture<MvPartitionStorage> create(int partitionId, IntFunction<T> createStorageFunction) {
        StorageOperation storageOperation = operationByPartitionId.compute(partitionId, (partId, operation) -> {
            if (operation instanceof DestroyStorageOperation) {
                if (!((DestroyStorageOperation) operation).setCreationOperation(new CreateStorageOperation())) {
                    throw new StorageException(
                            "Creation of the storage after its destruction is already planned: [" + createStorageInfo(partitionId) + ']'
                    );
                }

                return operation;
            }

            if (get(partitionId) != null) {
                throw new StorageException("Storage already exists: [" + createStorageInfo(partitionId) + ']');
            }

            if (operation != null) {
                throwExceptionDependingOnOperation(operation, partitionId);
            }

            return new CreateStorageOperation();
        });

        CompletableFuture<Void> destroyStorageFuture = storageOperation instanceof DestroyStorageOperation
                ? ((DestroyStorageOperation) storageOperation).getDestroyFuture()
                : completedFuture(null);

        return destroyStorageFuture.thenApply(unused -> {
            T newStorage = createStorageFunction.apply(partitionId);

            boolean set = storageByPartitionId.compareAndSet(partitionId, null, newStorage);

            assert set : createStorageInfo(partitionId);

            return (MvPartitionStorage) newStorage;
        }).whenComplete((storage, throwable) -> operationByPartitionId.compute(partitionId, (partId, operation) -> {
            assert operation instanceof CreateStorageOperation : createStorageInfo(partitionId) + ", op=" + operation;

            return null;
        }));
    }

    /**
     * Destroys a multi-versioned partition storage.
     *
     * @param partitionId Partition ID.
     * @param destroyStorageFunction Partition destruction function.
     * @return Future of multi-versioned partition storage destruction.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageException If the storage does not exist or another operation is already in progress.
     * @throws StorageRebalanceException If the storage is in the process of rebalancing.
     */
    public CompletableFuture<Void> destroy(int partitionId, Function<T, CompletableFuture<Void>> destroyStorageFunction) {
        DestroyStorageOperation destroyOp = (DestroyStorageOperation) operationByPartitionId.compute(partitionId, (partId, operation) -> {
            checkStorageExists(partitionId);

            if (operation != null) {
                throwExceptionDependingOnOperation(operation, partitionId);
            }

            return new DestroyStorageOperation();
        });

        return completedFuture(null)
                .thenCompose(unused -> destroyStorageFunction.apply(storageByPartitionId.getAndSet(partitionId, null)))
                .whenComplete((unused, throwable) -> {
                    operationByPartitionId.compute(partitionId, (partId, operation) -> {
                        assert operation instanceof DestroyStorageOperation : createStorageInfo(partitionId) + ", op=" + operation;

                        DestroyStorageOperation destroyStorageOperation = (DestroyStorageOperation) operation;

                        return destroyStorageOperation.getCreateStorageOperation();
                    });

                    if (throwable == null) {
                        destroyOp.getDestroyFuture().complete(null);
                    } else {
                        destroyOp.getDestroyFuture().completeExceptionally(throwable);
                    }
                });
    }

    /**
     * Clears a multi-versioned partition storage.
     *
     * @param partitionId Partition ID.
     * @param clearStorageFunction Partition clean up function.
     * @return Future of cleaning a multi-versioned partition storage.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageException If the storage does not exist or another operation is already in progress.
     * @throws StorageRebalanceException If the storage is in the process of rebalancing.
     */
    public CompletableFuture<Void> clear(int partitionId, Function<T, CompletableFuture<Void>> clearStorageFunction) {
        operationByPartitionId.compute(partitionId, (partId, operation) -> {
            checkStorageExists(partitionId);

            if (operation != null) {
                throwExceptionDependingOnOperation(operation, partitionId);
            }

            return new CleanupStorageOperation();
        });

        return completedFuture(null)
                .thenCompose(unused -> clearStorageFunction.apply(get(partitionId)))
                .whenComplete((unused, throwable) ->
                        operationByPartitionId.compute(partitionId, (partId, operation) -> {
                            assert operation instanceof CleanupStorageOperation : createStorageInfo(partitionId) + ", op=" + operation;

                            return null;
                        })
                );
    }

    /**
     * Starts a multi-versioned partition storage rebalance.
     *
     * @param partitionId Partition ID.
     * @param startRebalanceStorageFunction Partition start rebalance function.
     * @return Future of starting rebalance a multi-versioned partition storage.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageRebalanceException If the storage does not exist or another operation is already in progress.
     * @throws StorageRebalanceException If rebalancing is already in progress.
     */
    public CompletableFuture<Void> startRebalace(int partitionId, Function<T, CompletableFuture<Void>> startRebalanceStorageFunction) {
        operationByPartitionId.compute(partitionId, (partId, operation) -> {
            checkStorageExistsForRebalance(partitionId);

            if (operation != null) {
                throwExceptionDependingOnOperationForRebalance(operation, partitionId);
            }

            if (rebalaceFutureByPartitionId.containsKey(partitionId)) {
                throw new StorageRebalanceException(createStorageInProgressOfRebalanceErrorMessage(partitionId));
            }

            return new StartRebalanceStorageOperation();
        });

        return completedFuture(null)
                .thenCompose(unused -> {
                    CompletableFuture<Void> startRebalanceFuture = startRebalanceStorageFunction.apply(get(partitionId));

                    CompletableFuture<Void> old = rebalaceFutureByPartitionId.put(partitionId, startRebalanceFuture);

                    assert old == null : createStorageInfo(partitionId);

                    return startRebalanceFuture;
                }).whenComplete((unused, throwable) ->
                        operationByPartitionId.compute(partitionId, (partId, operation) -> {
                            assert operation instanceof StartRebalanceStorageOperation :
                                    createStorageInfo(partitionId) + ", op=" + operation;

                            return null;
                        })
                );
    }

    /**
     * Aborts a multi-versioned partition storage rebalance if started (successful or not).
     *
     * @param partitionId Partition ID.
     * @param abortRebalanceStorageFunction Partition abort rebalance function.
     * @return Future of aborting rebalance a multi-versioned partition storage.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageRebalanceException If the storage does not exist or another operation is already in progress.
     */
    public CompletableFuture<Void> abortRebalance(int partitionId, Function<T, CompletableFuture<Void>> abortRebalanceStorageFunction) {
        operationByPartitionId.compute(partitionId, (partId, operation) -> {
            checkStorageExistsForRebalance(partitionId);

            if (operation != null) {
                throwExceptionDependingOnOperationForRebalance(operation, partitionId);
            }

            return new AbortRebalanceStorageOperation();
        });

        return completedFuture(null)
                .thenCompose(unused -> {
                    CompletableFuture<Void> rebalanceFuture = rebalaceFutureByPartitionId.remove(partitionId);

                    if (rebalanceFuture == null) {
                        return completedFuture(null);
                    }

                    return rebalanceFuture
                            .handle((unused1, throwable) -> abortRebalanceStorageFunction.apply(get(partitionId)))
                            .thenCompose(identity());
                }).whenComplete((unused, throwable) ->
                        operationByPartitionId.compute(partitionId, (partId, operation) -> {
                            assert operation instanceof AbortRebalanceStorageOperation :
                                    createStorageInfo(partitionId) + ", op=" + operation;

                            return null;
                        })
                );
    }

    /**
     * Finishes a successful started multi-versioned partition storage rebalance.
     *
     * @param partitionId Partition ID.
     * @param finishRebalanceStorageFunction Partition finish rebalance function, the argument is the partition ID.
     * @return Future of aborting rebalance a multi-versioned partition storage.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageRebalanceException If the storage does not exist or another operation is already in progress.
     * @throws StorageRebalanceException If storage rebalancing has not started.
     */
    public CompletableFuture<Void> finishRebalance(int partitionId, Function<T, CompletableFuture<Void>> finishRebalanceStorageFunction) {
        operationByPartitionId.compute(partitionId, (partId, operation) -> {
            checkStorageExistsForRebalance(partitionId);

            if (operation != null) {
                throwExceptionDependingOnOperationForRebalance(operation, partitionId);
            }

            if (!rebalaceFutureByPartitionId.containsKey(partitionId)) {
                throw new StorageRebalanceException("Storage rebalancing did not start: [" + createStorageInfo(partitionId) + ']');
            }

            return new FinishRebalanceStorageOperation();
        });

        return completedFuture(null)
                .thenCompose(unused -> {
                    CompletableFuture<Void> rebalanceFuture = rebalaceFutureByPartitionId.remove(partitionId);

                    assert rebalanceFuture != null : createStorageInfo(partitionId);

                    return rebalanceFuture.thenCompose(unused1 -> finishRebalanceStorageFunction.apply(get(partitionId)));
                }).whenComplete((unused, throwable) ->
                        operationByPartitionId.compute(partitionId, (partId, operation) -> {
                            assert operation instanceof FinishRebalanceStorageOperation :
                                    createStorageInfo(partitionId) + ", op=" + operation;

                            return null;
                        })
                );
    }

    /**
     * Collects all multi-versioned partition storages to close.
     */
    // TODO: IGNITE-18529 We need to wait for all current operations and disable new ones
    public List<T> getAllForClose() {
        return IntStream.range(0, storageByPartitionId.length())
                .mapToObj(partitionId -> storageByPartitionId.getAndSet(partitionId, null))
                .filter(Objects::nonNull)
                .collect(toList());
    }

    /**
     * Destroys all created multi-versioned partition storages.
     *
     * @param destroyStorageFunction Partition destruction function.
     * @return Future destruction of all created multi-versioned partition storages.
     */
    // TODO: IGNITE-18529 We need to deal with parallel operations
    public CompletableFuture<Void> destroyAll(Function<T, CompletableFuture<Void>> destroyStorageFunction) {
        List<CompletableFuture<Void>> destroyFutures = new ArrayList<>();

        for (int partitionId = 0; partitionId < storageByPartitionId.length(); partitionId++) {
            StorageOperation storageOperation = operationByPartitionId.get(partitionId);

            if (storageOperation instanceof DestroyStorageOperation) {
                destroyFutures.add(((DestroyStorageOperation) storageOperation).getDestroyFuture());
            } else {
                T storage = storageByPartitionId.getAndSet(partitionId, null);

                if (storage != null) {
                    destroyFutures.add(destroyStorageFunction.apply(storage));
                }
            }
        }

        return CompletableFuture.allOf(destroyFutures.toArray(CompletableFuture[]::new));
    }

    /**
     * Returns table name.
     */
    public String getTableName() {
        return tableView.name();
    }

    /**
     * Creates a short info of the multi-versioned partition storage in the format "table=user, partitionId=1".
     *
     * @param partitionId Partition ID.
     */
    public String createStorageInfo(int partitionId) {
        return IgniteStringFormatter.format("table={}, partitionId={}", getTableName(), partitionId);
    }

    /**
     * Checks that the partition ID is within the scope of the configuration.
     *
     * @param partitionId Partition ID.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     */
    private void checkPartitionId(int partitionId) {
        int partitions = storageByPartitionId.length();

        if (partitionId < 0 || partitionId >= partitions) {
            throw new IllegalArgumentException(IgniteStringFormatter.format(
                    "Unable to access partition with id outside of configured range: [table={}, partitionId={}, partitions={}]",
                    getTableName(),
                    partitionId,
                    partitions
            ));
        }
    }

    /**
     * Checks if the storage exists.
     *
     * @param partitionId Partition ID.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageException If the storage does not exist.
     */
    private void checkStorageExists(int partitionId) {
        if (get(partitionId) == null) {
            throw new StorageException(createStorageDoesNotExistErrorMessage(partitionId));
        }
    }

    /**
     * Checks if the storage exists.
     *
     * @param partitionId Partition ID.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageRebalanceException If the storage does not exist.
     */
    private void checkStorageExistsForRebalance(int partitionId) {
        if (get(partitionId) == null) {
            throw new StorageRebalanceException(createStorageDoesNotExistErrorMessage(partitionId));
        }
    }

    private void throwExceptionDependingOnOperation(StorageOperation operation, int partitionId) {
        if (operation instanceof CreateStorageOperation) {
            throw new StorageException(createStorageInProgressOfCreationErrorMessage(partitionId));
        } else if (operation instanceof DestroyStorageOperation) {
            throw new StorageException(createStorageInProgressOfDestructionErrorMessage(partitionId));
        } else if (operation instanceof StartRebalanceStorageOperation) {
            throw new StorageRebalanceException(createStorageInProgressOfStartRebalanceErrorMessage(partitionId));
        } else if (operation instanceof AbortRebalanceStorageOperation) {
            throw new StorageRebalanceException(createStorageInProgressOfAbortRebalanceErrorMessage(partitionId));
        } else if (operation instanceof FinishRebalanceStorageOperation) {
            throw new StorageRebalanceException(createStorageInProgressOfFinishRebalanceErrorMessage(partitionId));
        } else if (operation instanceof CleanupStorageOperation) {
            throw new StorageException(createStorageInProgressOfCleanupErrorMessage(partitionId));
        } else {
            throw new StorageException(createUnknownOperationErrorMessage(partitionId, operation));
        }
    }

    private void throwExceptionDependingOnOperationForRebalance(StorageOperation operation, int partitionId) {
        if (operation instanceof CreateStorageOperation) {
            throw new StorageRebalanceException(createStorageInProgressOfCreationErrorMessage(partitionId));
        } else if (operation instanceof DestroyStorageOperation) {
            throw new StorageRebalanceException(createStorageInProgressOfDestructionErrorMessage(partitionId));
        } else if (operation instanceof StartRebalanceStorageOperation) {
            throw new StorageRebalanceException(createStorageInProgressOfStartRebalanceErrorMessage(partitionId));
        } else if (operation instanceof AbortRebalanceStorageOperation) {
            throw new StorageRebalanceException(createStorageInProgressOfAbortRebalanceErrorMessage(partitionId));
        } else if (operation instanceof FinishRebalanceStorageOperation) {
            throw new StorageRebalanceException(createStorageInProgressOfFinishRebalanceErrorMessage(partitionId));
        } else if (operation instanceof CleanupStorageOperation) {
            throw new StorageRebalanceException(createStorageInProgressOfCleanupErrorMessage(partitionId));
        } else {
            throw new StorageRebalanceException(createUnknownOperationErrorMessage(partitionId, operation));
        }
    }

    private String createStorageDoesNotExistErrorMessage(int partitionId) {
        return "Storage does not exist: [" + createStorageInfo(partitionId) + ']';
    }

    private String createStorageInProgressOfCreationErrorMessage(int partitionId) {
        return "Storage is in process of being created: [" + createStorageInfo(partitionId) + ']';
    }

    private String createStorageInProgressOfDestructionErrorMessage(int partitionId) {
        return "Storage is already in process of being destroyed: [" + createStorageInfo(partitionId) + ']';
    }

    private String createStorageInProgressOfStartRebalanceErrorMessage(int partitionId) {
        return "Storage in the process of starting a rebalance: [" + createStorageInfo(partitionId) + ']';
    }

    private String createStorageInProgressOfAbortRebalanceErrorMessage(int partitionId) {
        return "Storage in the process of aborting a rebalance: [" + createStorageInfo(partitionId) + ']';
    }

    private String createStorageInProgressOfFinishRebalanceErrorMessage(int partitionId) {
        return "Storage in the process of finishing a rebalance: [" + createStorageInfo(partitionId) + ']';
    }

    private String createStorageInProgressOfRebalanceErrorMessage(int partitionId) {
        return "Storage in the process of rebalance: [" + createStorageInfo(partitionId) + ']';
    }

    private String createStorageInProgressOfCleanupErrorMessage(int partitionId) {
        return "Storage is in process of being cleaned up: [" + createStorageInfo(partitionId) + ']';
    }

    private String createUnknownOperationErrorMessage(int partitionId, StorageOperation operation) {
        return "Unknown operation: [" + createStorageInfo(partitionId) + ", operation=" + operation + ']';
    }
}
