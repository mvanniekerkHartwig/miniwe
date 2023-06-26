package com.hartwig.miniwe.kubernetes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class KubernetesUtilTest {
    @Test
    void validInputShouldReturnValidLabel() {
        assertThat(KubernetesUtil.toValidRFC1123Label("Valid", "Label")).isEqualTo("valid-label");
    }

    @Test
    void inputExceedingMaxLengthShouldThrow() {
        String[] input =
                { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x",
                        "y", "z", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u",
                        "v", "w", "x", "y", "z", "a", "b", "c", "d", "e", "f" };
        assertThrows(IllegalArgumentException.class, () -> KubernetesUtil.toValidRFC1123Label(input));
    }

    @Test
    void invalidInputShouldThrow() {
        assertThrows(IllegalArgumentException.class, () -> KubernetesUtil.toValidRFC1123Label(new String[] { "Invalid@", "Label" }));
    }
}