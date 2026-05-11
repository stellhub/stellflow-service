package io.github.stellhub.stellflow.network.protocol;

/**
 * 已中止事务占位结构。
 */
public record AbortedTransaction(long producerId, long firstOffset) {}
