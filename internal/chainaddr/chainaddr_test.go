package chainaddr

import "testing"

func TestNormalizeAndDetect(t *testing.T) {
	cases := []struct{ in, chain, want string }{
		{"TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", "tron", "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"},
		{"EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs", "ton", "0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe"},
		{"UQCdssroU88tuHDqBtHoKG8gLNmtaJ38dLmBlVINBklYWYEg", "ton", "0:9db2cae853cf2db870ea06d1e8286f202cd9ad689dfc74b98195520d06495859"},
		{" 0:9DB2CAE853CF2DB870EA06D1E8286F202CD9AD689DFC74B98195520D06495859 ", "ton", "0:9db2cae853cf2db870ea06d1e8286f202cd9ad689dfc74b98195520d06495859"},
	}
	for _, c := range cases {
		got, err := Detect(c.in)
		if err != nil || got != c.chain {
			t.Errorf("Detect(%q) = %q, %v", c.in, got, err)
		}
		n, err := Normalize(c.chain, c.in)
		if err != nil || n != c.want {
			t.Errorf("Normalize(%q) = %q, %v", c.in, n, err)
		}
	}
	if _, err := Normalize("ton", "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"); err == nil {
		t.Error("tron address accepted as ton")
	}
	if _, err := Normalize("tron", "EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs"); err == nil {
		t.Error("ton address accepted as tron")
	}
	if _, err := Normalize("eth", "x"); err == nil {
		t.Error("unknown chain accepted")
	}
	if _, err := Detect("hello"); err == nil {
		t.Error("garbage detected")
	}
	if !Valid("ton") || !Valid("tron") || Valid("") {
		t.Error("Valid")
	}
}

func TestETHDetectAndNormalize(t *testing.T) {
	c, err := Detect("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed")
	if err != nil || c != "eth" {
		t.Fatal(c, err)
	}
	if _, err := Detect("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAeD"); err == nil {
		t.Fatal("bad checksum must not be detected")
	}
	n, err := Normalize("eth", "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed")
	if err != nil || n != "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed" {
		t.Fatal(n, err)
	}
	if !Valid("eth") {
		t.Fatal()
	}
}
