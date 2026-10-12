package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/model"
)

func tonTransfer() model.Transfer {
	return model.Transfer{
		Chain: model.ChainTON, TxID: "540a39b8299cc5d0239eefa2be4268445a262b897a6cd970f1fb2ac4e70aef19",
		BlockNumber: 98632901, BlockTime: time.Unix(1791840598, 0).UTC(),
		Contract: "0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe", Symbol: "USDT", Decimals: 6,
		From:   "0:9db2cae853cf2db870ea06d1e8286f202cd9ad689dfc74b98195520d06495859",
		To:     "0:6fc4b8ae3a3c8bbf09a0ee6dd07be32aabc0b661d017a82f5d28675c9bd7ab81",
		Amount: big.NewInt(11_250_000), Memo: "inv <42>", Reasons: []string{model.ReasonIncoming},
	}
}

func TestFormatTelegramTON(t *testing.T) {
	msg := FormatTelegram(tonTransfer(), "https://tonviewer.com/transaction/")
	for _, want := range []string{
		"📥 Incoming USDT", "11.25 USDT",
		"<code>UQCdssroU88tuHDqBtHoKG8gLNmtaJ38dLmBlVINBklYWYEg</code>",
		"<code>UQBvxLiuOjyLvwmg7m3Qe-Mqq8C2YdAXqC9dKGdcm9ergbiQ</code>",
		"Memo: <code>inv &lt;42&gt;</code>",
		"TON masterchain block: 98632901",
		`href="https://tonviewer.com/transaction/540a39b8299cc5d0239eefa2be4268445a262b897a6cd970f1fb2ac4e70aef19"`,
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("missing %q:\n%s", want, msg)
		}
	}
	if got := truncRunes(strings.Repeat("я", 300), 200); len([]rune(got)) != 201 {
		t.Errorf("truncRunes len %d", len([]rune(got)))
	}
}

func TestStdoutAndJSONIncludeChain(t *testing.T) {
	var buf bytes.Buffer
	tr := tonTransfer()
	if err := NewStdout(&buf).Send(context.Background(), Batch{Transfers: []model.Transfer{tr}}); err != nil {
		t.Fatal(err)
	}
	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatal(err)
	}
	if line["chain"] != "ton" || line["memo"] != "inv <42>" || line["id"] != "ton:"+tr.TxID+":0" {
		t.Errorf("stdout line %v", line)
	}
	b, _ := json.Marshal(tr)
	var back model.Transfer
	if err := json.Unmarshal(b, &back); err != nil || back.Chain != "ton" || back.Memo != tr.Memo || back.Key() != tr.Key() {
		t.Errorf("json round trip %s %+v %v", b, back, err)
	}
	tron := sampleBatch().Transfers[0]
	b, _ = json.Marshal(tron)
	if !strings.Contains(string(b), `"chain":"tron"`) || strings.Contains(string(b), "memo") {
		t.Errorf("tron json %s", b)
	}
}
