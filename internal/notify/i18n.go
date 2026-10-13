package notify

import "strings"

// Languages supported by the bot. The user's Telegram client language
// (message.from.language_code) picks one; anything else falls back to en.
const (
	LangEN = "en"
	LangZH = "zh"
	LangRU = "ru"
)

// ProjectURL is shown in /start.
const ProjectURL = "https://github.com/co-codin/USDT-Tracker"

// langOf maps a Telegram language_code to a supported language.
func langOf(code string) string {
	code = strings.ToLower(code)
	switch {
	case strings.HasPrefix(code, "zh"):
		return LangZH
	case strings.HasPrefix(code, "ru"), strings.HasPrefix(code, "uk"), strings.HasPrefix(code, "be"), strings.HasPrefix(code, "kk"):
		return LangRU
	default:
		return LangEN
	}
}

type msgKey int

const (
	mStart msgKey = iota
	mHelp
	mFreePlan
	mHint
	mTempError
	mPrivateOnly
	mUsageAdd
	mAlready
	mCapacity
	mLimit
	mWatching
	mNone
	mListHeader
	mUsageRemove
	mNoSuchNumber
	mNotWatching
	mStopped
	mPlan
	mPremiumHow
	mPremiumContact
	mUnknown
	mUnrecognised
	mUnsupported
	mInvalidAddr
	mReceived
	mSent
	mSelf
	mTo
	mFrom
	mAddress
	mMemo
	mView
)

// catalog holds every user-facing string (HTML; %-verbs are filled by fmt).
var catalog = map[string]map[msgKey]string{
	LangEN: {
		mStart: "👋 <b>USDT payment notifications</b>\nI message you whenever USDT arrives at (or leaves) your addresses on TRON, TON or Ethereum. " +
			"Read-only: I never ask for keys or seed phrases.\nOpen source: " + ProjectURL + "\n\nStart with /add &lt;address&gt;.",
		mHelp: "/add &lt;address&gt; – watch an address (network detected automatically)\n/list – your addresses\n" +
			"/remove &lt;address or number&gt; – stop watching\n/plan – your plan and limits\n/help – this message",
		mFreePlan:       "Free plan: up to %d addresses.",
		mHint:           "Send /add &lt;address&gt; or /help.",
		mTempError:      "⚠️ Temporary error, please try again later.",
		mPrivateOnly:    "🔒 Please message me in a private chat to manage your addresses.",
		mUsageAdd:       "Usage: /add &lt;address&gt;\nExamples: T…, UQ…/EQ… or 0x…",
		mAlready:        "ℹ️ Already watching this address.",
		mCapacity:       "⏳ This bot is at capacity right now and cannot accept new addresses. Please try again later.",
		mLimit:          "🔒 Your %s plan allows %d addresses. Remove one with /remove, or upgrade to premium for up to %d (see /plan).",
		mWatching:       "✅ Watching %s address\n<code>%s</code>\nYou'll get a message for every incoming and outgoing USDT transfer.",
		mNone:           "You are not watching any address yet. Send /add &lt;address&gt;.",
		mListHeader:     "📋 Your addresses (%d/%d, %s plan):",
		mUsageRemove:    "Usage: /remove &lt;address or number from /list&gt;",
		mNoSuchNumber:   "❌ No such number, see /list.",
		mNotWatching:    "ℹ️ You are not watching this address.",
		mStopped:        "🗑 Stopped watching <code>%s</code>.",
		mPlan:           "Plan: <b>%s</b>\nAddresses: %d/%d\nPremium: up to %d addresses.",
		mPremiumHow:     "Premium is enabled by the operator of this bot.",
		mPremiumContact: "To upgrade to premium, contact %s.",
		mUnknown:        "Unknown command. /help",
		mUnrecognised:   "unrecognised address: send a TRON (T…), TON (UQ…/EQ…) or Ethereum (0x…) address",
		mUnsupported:    "%s is not supported by this bot yet",
		mInvalidAddr:    "invalid %s address",
		mReceived:       "📥 <b>+%s</b> received on %s",
		mSent:           "📤 <b>−%s</b> sent on %s",
		mSelf:           "🔁 <b>%s</b> self-transfer on %s",
		mTo:             "To",
		mFrom:           "From",
		mAddress:        "Address",
		mMemo:           "Memo",
		mView:           "View transaction",
	},
	LangZH: {
		mStart: "👋 <b>USDT 到账通知</b>\n你的 TRON、TON 或以太坊地址有 USDT 转入或转出时，我会发消息提醒你。" +
			"只读：我绝不会索要私钥或助记词。\n开源项目：" + ProjectURL + "\n\n发送 /add &lt;地址&gt; 开始。",
		mHelp: "/add &lt;地址&gt; – 监听一个地址（自动识别网络）\n/list – 我的地址\n" +
			"/remove &lt;地址或序号&gt; – 取消监听\n/plan – 套餐和额度\n/help – 显示本帮助",
		mFreePlan:       "免费版：最多 %d 个地址。",
		mHint:           "发送 /add &lt;地址&gt; 或 /help。",
		mTempError:      "⚠️ 暂时出错了，请稍后再试。",
		mPrivateOnly:    "🔒 请在私聊中和我对话来管理你的地址。",
		mUsageAdd:       "用法：/add &lt;地址&gt;\n例如：T…、UQ…/EQ… 或 0x…",
		mAlready:        "ℹ️ 这个地址已经在监听了。",
		mCapacity:       "⏳ 机器人当前已满，暂时不能添加新地址，请稍后再试。",
		mLimit:          "🔒 你的 %s 套餐最多 %d 个地址。可以用 /remove 删掉一个，或升级到高级版（最多 %d 个，见 /plan）。",
		mWatching:       "✅ 已开始监听 %s 地址\n<code>%s</code>\n每笔 USDT 转入和转出都会通知你。",
		mNone:           "你还没有监听任何地址。发送 /add &lt;地址&gt;。",
		mListHeader:     "📋 你的地址（%d/%d，%s 套餐）：",
		mUsageRemove:    "用法：/remove &lt;地址或 /list 里的序号&gt;",
		mNoSuchNumber:   "❌ 没有这个序号，请看 /list。",
		mNotWatching:    "ℹ️ 你没有监听这个地址。",
		mStopped:        "🗑 已停止监听 <code>%s</code>。",
		mPlan:           "套餐：<b>%s</b>\n地址：%d/%d\n高级版：最多 %d 个地址。",
		mPremiumHow:     "高级版由机器人运营方开通。",
		mPremiumContact: "升级高级版请联系 %s。",
		mUnknown:        "未知命令。/help",
		mUnrecognised:   "无法识别的地址：请发送 TRON（T…）、TON（UQ…/EQ…）或以太坊（0x…）地址",
		mUnsupported:    "本机器人暂不支持 %s",
		mInvalidAddr:    "%s 地址无效",
		mReceived:       "📥 <b>+%s</b> 已到账（%s）",
		mSent:           "📤 <b>−%s</b> 已转出（%s）",
		mSelf:           "🔁 <b>%s</b> 自转账（%s）",
		mTo:             "收款",
		mFrom:           "付款",
		mAddress:        "地址",
		mMemo:           "备注",
		mView:           "查看交易",
	},
	LangRU: {
		mStart: "👋 <b>Уведомления о USDT-платежах</b>\nЯ пишу, когда USDT приходит на ваши адреса (или уходит с них) в TRON, TON или Ethereum. " +
			"Только чтение: я никогда не прошу ключи или сид-фразы.\nОткрытый код: " + ProjectURL + "\n\nНачните с /add &lt;адрес&gt;.",
		mHelp: "/add &lt;адрес&gt; – следить за адресом (сеть определяется автоматически)\n/list – ваши адреса\n" +
			"/remove &lt;адрес или номер&gt; – перестать следить\n/plan – тариф и лимиты\n/help – эта справка",
		mFreePlan:       "Бесплатный тариф: до %d адресов.",
		mHint:           "Отправьте /add &lt;адрес&gt; или /help.",
		mTempError:      "⚠️ Временная ошибка, попробуйте позже.",
		mPrivateOnly:    "🔒 Напишите мне в личные сообщения, чтобы управлять адресами.",
		mUsageAdd:       "Использование: /add &lt;адрес&gt;\nНапример: T…, UQ…/EQ… или 0x…",
		mAlready:        "ℹ️ Этот адрес уже отслеживается.",
		mCapacity:       "⏳ Бот сейчас заполнен и не принимает новые адреса. Попробуйте позже.",
		mLimit:          "🔒 Тариф %s позволяет %d адресов. Удалите один через /remove или перейдите на премиум (до %d, см. /plan).",
		mWatching:       "✅ Отслеживаю адрес %s\n<code>%s</code>\nВы получите сообщение о каждом входящем и исходящем переводе USDT.",
		mNone:           "Вы пока не отслеживаете ни одного адреса. Отправьте /add &lt;адрес&gt;.",
		mListHeader:     "📋 Ваши адреса (%d/%d, тариф %s):",
		mUsageRemove:    "Использование: /remove &lt;адрес или номер из /list&gt;",
		mNoSuchNumber:   "❌ Нет такого номера, см. /list.",
		mNotWatching:    "ℹ️ Вы не отслеживаете этот адрес.",
		mStopped:        "🗑 Больше не отслеживаю <code>%s</code>.",
		mPlan:           "Тариф: <b>%s</b>\nАдреса: %d/%d\nПремиум: до %d адресов.",
		mPremiumHow:     "Премиум подключает оператор бота.",
		mPremiumContact: "Чтобы перейти на премиум, напишите %s.",
		mUnknown:        "Неизвестная команда. /help",
		mUnrecognised:   "адрес не распознан: отправьте адрес TRON (T…), TON (UQ…/EQ…) или Ethereum (0x…)",
		mUnsupported:    "%s пока не поддерживается этим ботом",
		mInvalidAddr:    "неверный адрес %s",
		mReceived:       "📥 <b>+%s</b> получено в %s",
		mSent:           "📤 <b>−%s</b> отправлено в %s",
		mSelf:           "🔁 <b>%s</b> перевод самому себе в %s",
		mTo:             "Кому",
		mFrom:           "От",
		mAddress:        "Адрес",
		mMemo:           "Комментарий",
		mView:           "Открыть транзакцию",
	},
}

// tr returns the string for key in lang (falls back to English).
func tr(lang string, k msgKey) string {
	if s, ok := catalog[lang][k]; ok {
		return s
	}
	return catalog[LangEN][k]
}
