package segment

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestUnit_FooterV2_PayloadTamper_ReturnsChecksumMismatch(t *testing.T) {
	data := encodeFooterV2(makeFooterV2Fixture(1, 2))
	data[8] ^= 1

	if _, err := decodeFooterV2(data); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("decodeFooterV2 error = %v, want ErrChecksumMismatch", err)
	}
}

func TestUnit_FooterV2_CapsSummaryTamper_ReturnsChecksumMismatch(t *testing.T) {
	data := encodeFooterV2(makeFooterV2Fixture(1, 2))
	trailerStart := len(data) - LSG_FOOTER_TRAILER_SIZE
	data[trailerStart+4] ^= 1

	if _, err := decodeFooterV2(data); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("decodeFooterV2 error = %v, want ErrChecksumMismatch", err)
	}
}

func TestUnit_FooterV2_CapsSummaryMismatchWithValidCRC_ReturnsCorruptSegment(t *testing.T) {
	footer := makeFooterV2Fixture(1, 2)
	data := encodeFooterV2(footer)

	trailerStart := len(data) - LSG_FOOTER_TRAILER_SIZE
	binary.LittleEndian.PutUint32(data[trailerStart+4:trailerStart+8], footerCapsSummary(footer.RequiredCaps, footer.OptionalCaps)^1)
	rewriteFooterCRC(data)

	if _, err := decodeFooterV2(data); !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("decodeFooterV2 error = %v, want ErrCorruptSegment", err)
	}
}
