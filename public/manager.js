'use strict';

const form = document.querySelector('#create-form');
const targetFields = document.querySelector('#target-fields');
const button = document.querySelector('#create-button');
const message = document.querySelector('#create-message');
const list = document.querySelector('#nodes-list');

async function fillNetworkDefaults() {
  try {
    const response = await fetch('/api/network', { cache: 'no-store' });
    if (!response.ok) return;
    const { suggestedHost } = await response.json();
    if (!suggestedHost) return;
    document.querySelector('#node-host').value = suggestedHost;
    document.querySelector('#target-host').value = suggestedHost;
  } catch {
    // Mantém 127.0.0.1 como fallback quando a interface não puder ser detectada.
  }
}

document.querySelectorAll('input[name="mode"]').forEach((radio) => {
  radio.addEventListener('change', () => {
    targetFields.hidden = document.querySelector('input[name="mode"]:checked').value !== 'join';
  });
});

function nodeAddress(node) {
  return `${node.host}:${node.port}`;
}

async function loadNodes() {
  try {
    const response = await fetch('/api/nodes', { cache: 'no-store' });
    const nodes = await response.json();
    if (!nodes.length) {
      list.innerHTML = '<p class="empty-row">Nenhum nó foi criado.</p>';
      return;
    }
    list.replaceChildren(...nodes.map((state) => {
      const card = document.createElement('article');
      card.className = 'node-card';
      const predecessor = state.predecessor?.id ?? '—';
      const successor = state.successor?.id ?? '—';
      const primaryCount = (state.primaryFiles || []).length;
      const replicaCount = (state.replicaFiles || []).length;
      card.innerHTML = `
        <div class="node-card-id">${state.node.id}</div>
        <div>
          <strong>Nó ${state.node.id}</strong>
          <span>${nodeAddress(state.node)}</span>
        </div>
        <div class="node-links"><span>← ${predecessor}</span><span>${successor} →</span></div>
        <div class="node-files">${primaryCount} primários · ${replicaCount} réplicas</div>
        <a class="button secondary" href="http://${nodeAddress(state.node)}">Abrir painel</a>`;
      return card;
    }));
  } catch (error) {
    list.innerHTML = `<p class="global-error">Erro ao consultar os nós: ${error.message}</p>`;
  }
}

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  button.disabled = true;
  message.className = 'form-message';
  message.textContent = 'Iniciando…';
  const joinsExisting = document.querySelector('input[name="mode"]:checked').value === 'join';
  const body = {
    id: Number(document.querySelector('#node-id').value),
    host: document.querySelector('#node-host').value.trim(),
    port: Number(document.querySelector('#node-port').value),
    bootstrap: joinsExisting ? {
      id: Number(document.querySelector('#target-id').value),
      host: document.querySelector('#target-host').value.trim(),
      port: Number(document.querySelector('#target-port').value)
    } : null
  };

  try {
    const response = await fetch('/api/nodes', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body)
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error);
    message.textContent = `Nó ${result.node.id} iniciado na porta ${result.node.port}.`;
    document.querySelector('#node-port').value = result.node.port + 1;
    await loadNodes();
  } catch (error) {
    message.className = 'form-message error';
    message.textContent = error.message;
  } finally {
    button.disabled = false;
  }
});

document.querySelector('#refresh-button').addEventListener('click', loadNodes);
fillNetworkDefaults();
loadNodes();
setInterval(loadNodes, 5000);
