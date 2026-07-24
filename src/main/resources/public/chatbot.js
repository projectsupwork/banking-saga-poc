// Contextual assistant widget — local engine (no external LLM).
// Keep in sync with docs/chatbot.js (same structure; only KB/CONTEXT differ).
(function(){
'use strict';

var CONTEXT = document.getElementById('enrollmentId') ? 'payrollLoan' : 'transfer';

var STOPWORDS = ['the','a','an','of','to','in','on','at','for','from','with','without','and','or','that','who',
  'which','what','how','when','where','why','if','is','are','was','be','been','this','it','my','your','his','her',
  'me','you','i','do','does','did','have','has','had','can','could','need','about','as','by'];

function normalize(str){
  return String(str||'')
    .toLowerCase()
    .normalize('NFD').replace(/[̀-ͯ]/g,'')
    .replace(/[^a-z0-9\s]/g,' ')
    .split(/\s+/)
    .filter(function(t){ return t.length>1 && STOPWORDS.indexOf(t)===-1; });
}

var KB = {
  general: [
    {
      id:'stack',
      keywords:['stack','technology','technologies','language','framework','java','micronaut','kafka','sqs'],
      question:'Which stack is used?',
      answer:'Java 21 + Micronaut 4.4.3 (Netty). Kafka for messaging between the SAGA steps, and SQS (via LocalStack) for notifications in the last step.'
    },
    {
      id:'saga-general',
      keywords:['saga','choreography','pattern','architecture','orchestrator'],
      question:'What is the SAGA pattern?',
      answer:'It is the choreography pattern used here: each step reacts to an event published by the previous step via Kafka, with no central orchestrator coordinating everything.'
    },
    {
      id:'run-project',
      keywords:['run','start','launch','execute','docker','compose'],
      question:'How do I run the project?',
      answer:'"docker compose up -d" starts Kafka, LocalStack and the application. After ~60s, "./test-flow.sh" runs an end-to-end flow.'
    },
    {
      id:'who-are-you',
      keywords:['who','you','help','assistant','bot','work'],
      question:'What do you do?',
      answer:'I am a local assistant for this screen — I answer short questions about the flow, business rules and architecture, with no internet dependency.'
    }
  ],
  transfer: [
    {
      id:'how-transfer-works',
      keywords:['works','transfer','flow','process','asynchronous','async'],
      question:'How does the transfer work?',
      answer:'It is asynchronous: the API replies 202 immediately and a Kafka consumer executes the SAGA in 4 steps — validate balance, debit source, credit target and notify via SQS.'
    },
    {
      id:'saga-id',
      keywords:['sagaid','saga','protocolid','protocol','id','idempotency','trace','track'],
      question:'What are sagaId and protocolId?',
      answer:'sagaId (prefix SAGA-) guarantees internal idempotency across the SAGA steps. protocolId (prefix TRF-) is the ID the client uses to track the transfer.'
    },
    {
      id:'insufficient-balance',
      keywords:['balance','insufficient','missing','rejected','reject','canceled','cancel'],
      question:'What happens if the balance is insufficient?',
      answer:'Step 1 (balance validation) rejects the transfer before any debit. No account is changed and the status becomes CANCELED.'
    },
    {
      id:'step4-sqs',
      keywords:['step4','notification','sqs','failure','dlq'],
      question:'What is Step 4 / the SQS notification?',
      answer:'It is the notification sent via SQS (LocalStack). If it fails, the transfer is not undone — it is already committed; the failure is only logged, intended for a DLQ retry in production.'
    },
    {
      id:'test-accounts',
      keywords:['accounts','test','acc001','acc002','acc003','alice','bob','carol','balance'],
      question:'Which test accounts exist?',
      answer:'ACC-001 Alice Johnson ($5,000), ACC-002 Bob Smith ($1,000) and ACC-003 Carol Davis ($250).'
    },
    {
      id:'quick-scenarios',
      keywords:['scenarios','quick','shortcut','example'],
      question:'What are the quick scenarios?',
      answer:'Pre-configured shortcuts: Alice → Bob $500 (success), Carol → Alice $9,999 (insufficient balance) and Bob → Carol $200.'
    },
    {
      id:'processing',
      keywords:['processing','slow','pending','waiting'],
      question:'Why does the transfer stay PROCESSING?',
      answer:'Because processing is asynchronous: the API replies 202 immediately, but the Kafka consumer is still executing the SAGA steps in the background.'
    },
    {
      id:'canceled-failed',
      keywords:['canceled','failed','difference','error','status'],
      question:'What is the difference between CANCELED and FAILED?',
      answer:'CANCELED is a violated business rule (e.g. insufficient balance), handled normally. FAILED is an unexpected critical error that would require manual intervention.'
    },
    {
      id:'debit-credit',
      keywords:['debit','credit','lock','concurrency','source','target'],
      question:'How do the debit and credit work?',
      answer:'Step 2 debits the source account; Step 3 credits the target account. Each account has its own write lock to prevent race conditions.'
    },
    {
      id:'minimum-amount',
      keywords:['amount','minimum','maximum','limit'],
      question:'Is there a minimum transfer amount?',
      answer:'The amount must be positive (greater than $0.00) — it is a required field validated on the request.'
    }
  ],
  payrollLoan: [
    {
      id:'payroll-margin',
      keywords:['margin','payroll','35','limit','installment','income'],
      question:'What is the payroll margin?',
      answer:'It is the 35% cap of the informed monthly income that the installment may occupy. If exceeded, the contract is rejected and canceled in Step 1.'
    },
    {
      id:'interest-rate',
      keywords:['rate','interest','154','annuity','amortization'],
      question:'What is the interest rate?',
      answer:'Fixed rate of 1.54% per month, with annuity (fixed-installment) amortization — interest accrues on the outstanding balance each period.'
    },
    {
      id:'term',
      keywords:['term','months','installments','24','120','time'],
      question:'Which terms are allowed?',
      answer:'Between 24 and 120 months, as informed at origination.'
    },
    {
      id:'requested-amount',
      keywords:['amount','requested','maximum','minimum','much'],
      question:'How much can I borrow?',
      answer:'Between $100.00 and $100,000.00.'
    },
    {
      id:'monthly-deduction',
      keywords:['deduction','monthly','payroll','benefit','automatic','simulate'],
      question:'How does the monthly deduction work?',
      answer:'It is simulated automatically every 30s (compressed for the demo) and can also be triggered manually with the "Simulate next deduction" button.'
    },
    {
      id:'paid-off',
      keywords:['paid','off','payoff','ends','finished','completed'],
      question:'What happens when the contract is paid off?',
      answer:'The status changes to PAID_OFF once every installment is paid, and the simulate-deduction button is disabled.'
    },
    {
      id:'credit-disbursement',
      keywords:['credit','disbursement','account','receive','money'],
      question:'Who credits the loan amount?',
      answer:'Step 3 uses the very same credit service as the transfers — to the system, the disbursement is indistinguishable from a regular credit.'
    },
    {
      id:'enrollment',
      keywords:['enrollment','id','employment','benefit'],
      question:'What is the enrollment id?',
      answer:'It identifies the employment/benefit relationship (payroll) used for the deduction. It must be unique among open contracts.'
    },
    {
      id:'scenarios-loan',
      keywords:['scenarios','quick','example','shortcut'],
      question:'What are the quick scenarios?',
      answer:'"Alice · $5,000 over 36 months" (approved) and "Bob · margin exceeded" (rejected for exceeding 35% of the income).'
    },
    {
      id:'awaiting-disbursement',
      keywords:['awaiting','disbursement','status','active','initial'],
      question:'What is AWAITING_DISBURSEMENT?',
      answer:'It is the contract’s initial status, before Step 3 (crediting the amount to the account) completes.'
    }
  ]
};

function expandKeywords(entry){
  return entry.keywords;
}

function activeEntries(){
  return KB.general.concat(KB[CONTEXT] || []);
}

function bestMatch(userText){
  var tokens = normalize(userText);
  if(!tokens.length) return null;
  var entries = activeEntries();
  var contextSet = KB[CONTEXT] || [];
  var best = null, bestScore = 0;
  entries.forEach(function(entry){
    var kws = expandKeywords(entry);
    var qTokens = normalize(entry.question);
    var score = 0;
    tokens.forEach(function(t){
      kws.forEach(function(k){
        if(t===k || (t.length>=4 && k.indexOf(t)!==-1) || (k.length>=4 && t.indexOf(k)!==-1)) score++;
      });
      if(qTokens.indexOf(t)!==-1) score+=2;
    });
    var isSpecific = contextSet.indexOf(entry)!==-1;
    if(score>bestScore || (score===bestScore && score>0 && isSpecific && best && contextSet.indexOf(best)===-1)){
      bestScore = score; best = entry;
    }
  });
  return bestScore>0 ? best : null;
}

function suggestions(n){
  var pool = (KB[CONTEXT]||[]).length ? KB[CONTEXT] : KB.general;
  var picked = [];
  var copy = pool.slice();
  while(picked.length<n && copy.length){
    picked.push(copy.splice(Math.floor(Math.random()*copy.length),1)[0]);
  }
  return picked;
}

// ---- UI ----
var els = {};

function buildUI(){
  var root = document.createElement('div');
  root.id = 'cw-root';
  root.innerHTML =
    '<button id="cw-fab" aria-label="Open assistant">💬<span class="cw-badge" id="cw-badge">?</span></button>' +
    '<div id="cw-panel" hidden>' +
      '<div class="cw-header"><span class="cw-header-title">🤖 Assistant</span><button id="cw-close" aria-label="Close">✕</button></div>' +
      '<div class="cw-messages" id="cw-messages"></div>' +
      '<div class="cw-chips" id="cw-chips"></div>' +
      '<div class="cw-inputbar"><input id="cw-input" placeholder="Ask something..." autocomplete="off"><button id="cw-send" aria-label="Send">➤</button></div>' +
    '</div>';
  document.body.appendChild(root);

  els.fab = document.getElementById('cw-fab');
  els.badge = document.getElementById('cw-badge');
  els.panel = document.getElementById('cw-panel');
  els.messages = document.getElementById('cw-messages');
  els.chips = document.getElementById('cw-chips');
  els.input = document.getElementById('cw-input');
  els.send = document.getElementById('cw-send');

  els.fab.addEventListener('click', togglePanel);
  document.getElementById('cw-close').addEventListener('click', togglePanel);
  els.send.addEventListener('click', handleSend);
  els.input.addEventListener('keydown', function(e){
    if(e.key==='Enter' && !e.shiftKey){ e.preventDefault(); handleSend(); }
  });

  if(!localStorage.getItem('cw_seen')){
    els.badge.style.display = 'flex';
  } else {
    els.badge.style.display = 'none';
  }
}

var opened = false;
function togglePanel(){
  opened = !opened;
  els.panel.hidden = !opened;
  if(opened){
    localStorage.setItem('cw_seen','1');
    els.badge.style.display = 'none';
    if(!els.messages.childElementCount){
      addBotMessage('Hi! I can help with quick questions about this screen. Tap a suggestion or type your question.', true);
      renderChips(suggestions(4));
    }
    els.input.focus();
  }
}

function scrollToBottom(){
  els.messages.scrollTop = els.messages.scrollHeight;
}

function addUserMessage(text){
  var div = document.createElement('div');
  div.className = 'cw-msg user';
  div.textContent = text;
  els.messages.appendChild(div);
  scrollToBottom();
}

function addBotMessage(text, instant){
  var div = document.createElement('div');
  div.className = 'cw-msg bot';
  els.messages.appendChild(div);
  scrollToBottom();
  if(instant){
    div.textContent = text;
    return;
  }
  typewrite(div, text);
}

var typingTimer = null;
function typewrite(div, text){
  if(typingTimer) clearInterval(typingTimer);
  var i = 0;
  typingTimer = setInterval(function(){
    i += 2;
    div.textContent = text.slice(0, i);
    scrollToBottom();
    if(i>=text.length){ clearInterval(typingTimer); typingTimer=null; }
  }, 18);
}

function addTypingIndicator(){
  var div = document.createElement('div');
  div.className = 'cw-msg bot';
  div.innerHTML = '<span class="cw-typing"><span></span><span></span><span></span></span>';
  els.messages.appendChild(div);
  scrollToBottom();
  return div;
}

function renderChips(entries){
  els.chips.innerHTML = '';
  entries.forEach(function(entry){
    var chip = document.createElement('button');
    chip.className = 'cw-chip';
    chip.textContent = entry.question;
    chip.addEventListener('click', function(){ ask(entry.question); });
    els.chips.appendChild(chip);
  });
}

function ask(text){
  text = text.trim();
  if(!text) return;
  addUserMessage(text);
  els.input.value = '';
  els.chips.innerHTML = '';
  var indicator = addTypingIndicator();
  var match = bestMatch(text);
  var delay = 600 + Math.random()*600;
  setTimeout(function(){
    indicator.remove();
    if(match){
      addBotMessage(match.answer);
    } else {
      var sugg = suggestions(3);
      addBotMessage('I don’t know how to answer that yet. Try asking about one of these topics:');
      renderChips(sugg);
    }
  }, delay);
}

function handleSend(){
  ask(els.input.value);
}

if(document.readyState==='loading'){
  document.addEventListener('DOMContentLoaded', buildUI);
} else {
  buildUI();
}

})();
