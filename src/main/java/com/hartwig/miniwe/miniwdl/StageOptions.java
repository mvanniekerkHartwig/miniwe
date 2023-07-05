package com.hartwig.miniwe.miniwdl;

import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@Value.Immutable
@Value.Style(jdkOnly = true)
@JsonDeserialize(as = ImmutableStageOptions.class)
@JsonSerialize(as = ImmutableStageOptions.class)
public interface StageOptions {
    Optional<String> serviceAccount();

    @Value.Default
    default boolean output() {
        return true;
    }

    static ImmutableStageOptions.Builder builder() {
        return ImmutableStageOptions.builder();
    }
}
