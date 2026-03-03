package server

import "testing"

func TestShouldOverrideBitmap(t *testing.T) {
	tests := []struct {
		name       string
		bitmapCard uint64
		totalRows  int64
		threshold  float64
		want       bool
	}{
		{
			name:       "100% selectivity exceeds 0.9 threshold",
			bitmapCard: 1000,
			totalRows:  1000,
			threshold:  0.9,
			want:       true,
		},
		{
			name:       "95% selectivity exceeds 0.9 threshold",
			bitmapCard: 950,
			totalRows:  1000,
			threshold:  0.9,
			want:       true,
		},
		{
			name:       "90% selectivity does not exceed 0.9 threshold (not strictly greater)",
			bitmapCard: 900,
			totalRows:  1000,
			threshold:  0.9,
			want:       false,
		},
		{
			name:       "50% selectivity below 0.9 threshold",
			bitmapCard: 500,
			totalRows:  1000,
			threshold:  0.9,
			want:       false,
		},
		{
			name:       "threshold 0.0 disables gate",
			bitmapCard: 1000,
			totalRows:  1000,
			threshold:  0.0,
			want:       false,
		},
		{
			name:       "threshold 1.0 always uses bitmap",
			bitmapCard: 1000,
			totalRows:  1000,
			threshold:  1.0,
			want:       false,
		},
		{
			name:       "totalRows 0 guards against division by zero",
			bitmapCard: 100,
			totalRows:  0,
			threshold:  0.9,
			want:       false,
		},
		{
			name:       "negative threshold disables gate",
			bitmapCard: 1000,
			totalRows:  1000,
			threshold:  -0.5,
			want:       false,
		},
		{
			name:       "small segment with high selectivity",
			bitmapCard: 10,
			totalRows:  10,
			threshold:  0.9,
			want:       true,
		},
		{
			name:       "just above threshold",
			bitmapCard: 901,
			totalRows:  1000,
			threshold:  0.9,
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldOverrideBitmap(tt.bitmapCard, tt.totalRows, tt.threshold)
			if got != tt.want {
				selectivity := float64(0)
				if tt.totalRows > 0 {
					selectivity = float64(tt.bitmapCard) / float64(tt.totalRows)
				}
				t.Errorf("shouldOverrideBitmap(%d, %d, %.2f) = %v, want %v (selectivity=%.4f)",
					tt.bitmapCard, tt.totalRows, tt.threshold, got, tt.want, selectivity)
			}
		})
	}
}
