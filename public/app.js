'use strict';

const elements = {
  connectionDot: document.querySelector('#connection-dot'),
  connectionLabel: document.querySelector('#connection-label'),
  nodeTitle: document.querySelector('#node-title'),
  nodeAddress: document.querySelector('#node-address'),
  currentId: document.querySelector('#current-id'),
  joinedLabel: document.querySelector('#joined-label'),
  predecessorId: document.querySelector('#predecessor-id'),
  predecessorAddress: document.querySelector('#predecessor-address'),
  successorId: document.querySelector('#successor-id'),
  successorAddress: document.querySelector('#successor-address'),
  successorsCount: document.querySelector('#successors-count'),
  successorsList: document.querySelector('#successors-list'),
  fingerBody: document.querySelector('#finger-body'),
  ring: document.querySelector('#ring'),
  lastUpdate: document.querySelector('#last-update'),
  joinPanel: document.querySelector('#join-panel'),
  joinForm: document.querySelector('#join-form'),
  joinButton: document.querySelector('#join-button'),
  joinMessage: document.querySelector('#join-message'),
  bootstrapFields: document.querySelector('#bootstrap-fields'),
  globalError: document.querySelector('#global-error'),
  refreshButton: document.querySelector('#refresh-button'),
  filesPanel: document.querySelector('#files-panel'),
  filesRefreshButton: document.querySelector('#files-refresh-button'),
  catalogList: document.querySelector('#catalog-list'),
  uploadForm: document.querySelector('#upload-form'),
  uploadFile: document.querySelector('#upload-file'),
  selectedFile: document.querySelector('#selected-file'),
  uploadButton: document.querySelector('#upload-button'),
  uploadMessage: document.querySelector('#upload-message'),
  storagePanel: document.querySelector('#storage-panel'),
  primaryFilesList: document.querySelector('#primary-files-list'),
  replicaFilesList: document.querySelector('#replica-files-list')
};

let refreshing = false;
let catalogRefreshing = false;

function address(node) {
  return node ? `${node.host}:${node.port}` : 'Não definido';
}

function renderNode(targetId, targetAddress, node) {
  targetId.textContent = node?.id ?? '—';
  targetAddress.textContent = address(node);
}

function renderRing(state) {
  const routeIds = new Set(state.fingerTable.flatMap((finger) =>
    finger.node ? [finger.node.id] : []));
  if (state.predecessor) routeIds.add(state.predecessor.id);
  if (state.successor) routeIds.add(state.successor.id);

  elements.ring.replaceChildren(...Array.from({ length: 32 }, (_, index) => {
    const id = index + 1;
    const angle = ((id - 1) / 32) * 360 - 90;
    const marker = document.createElement('span');
    marker.className = `ring-id${routeIds.has(id) ? ' route' : ''}${id === state.node.id ? ' self' : ''}`;
    marker.textContent = id;
    marker.title = id === state.node.id ? `Nó atual: ${id}` : `ID ${id}`;
    marker.style.transform = `rotate(${angle}deg) translateX(var(--ring-radius)) rotate(${-angle}deg)`;
    return marker;
  }));
}

function renderTable(fingers) {
  elements.fingerBody.replaceChildren(...fingers.map((finger) => {
    const row = document.createElement('tr');
    const values = [
      finger.index,
      finger.start,
      finger.node?.id ?? '—',
      address(finger.node)
    ];
    values.forEach((value, index) => {
      const cell = document.createElement('td');
      if (index === 2 && finger.node) {
        const badge = document.createElement('span');
        badge.className = 'node-badge';
        badge.textContent = value;
        cell.append(badge);
      } else {
        cell.textContent = value;
      }
      row.append(cell);
    });
    return row;
  }));
}

function render(state) {
  elements.nodeTitle.textContent = state.node.id;
  elements.currentId.textContent = state.node.id;
  elements.nodeAddress.textContent = `http://${address(state.node)}`;
  elements.joinedLabel.textContent = state.joined ? 'No anel' : 'Fora do anel';
  elements.joinedLabel.classList.toggle('active', state.joined);
  elements.joinPanel.hidden = state.joined;
  elements.filesPanel.hidden = !state.joined;
  elements.storagePanel.hidden = !state.joined;
  renderNode(elements.predecessorId, elements.predecessorAddress, state.predecessor);
  renderNode(elements.successorId, elements.successorAddress, state.successor);
  renderSuccessors(state.successors || []);
  renderLocalFiles(elements.primaryFilesList, state.primaryFiles || [], 'primary');
  renderLocalFiles(elements.replicaFilesList, state.replicaFiles || [], 'replica');
  renderTable(state.fingerTable);
  renderRing(state);
  elements.lastUpdate.textContent = `Atualizado às ${new Date().toLocaleTimeString('pt-BR')}`;
}

function renderSuccessors(successors) {
  elements.successorsCount.textContent = successors.length;
  elements.successorsList.textContent = successors.length
    ? successors.map((node) => node.id).join(' → ')
    : 'Lista vazia';
}

function renderLocalFiles(container, files, kind) {
  const empty = kind === 'primary'
    ? 'Nenhum arquivo primário neste nó.'
    : 'Nenhuma réplica neste nó.';
  if (!files.length) {
    container.innerHTML = `<p class="empty-row">${empty}</p>`;
    return;
  }

  container.replaceChildren(...files.map((file) => {
    const item = document.createElement('div');
    item.className = `storage-file ${kind}`;
    const title = document.createElement('strong');
    title.textContent = file.name;
    const meta = document.createElement('span');
    const parts = [];
    if (file.hashId != null) parts.push(`hash ${file.hashId}`);
    if (file.size != null) parts.push(`${file.size} bytes`);
    if (file.primaryNodeId != null) parts.push(`dono ${file.primaryNodeId}`);
    meta.textContent = parts.join(' · ');
    item.append(title, meta);
    return item;
  }));
}

function renderCatalog(names) {
  if (names.length === 0) {
    elements.catalogList.innerHTML = '<p class="empty-row">Nenhum arquivo foi inserido na rede.</p>';
    return;
  }

  elements.catalogList.replaceChildren(...names.map((name) => {
    const link = document.createElement('a');
    link.className = 'catalog-file';
    link.href = `/api/files?name=${encodeURIComponent(name)}`;
    link.download = name;

    const icon = document.createElement('span');
    icon.className = 'file-icon';
    icon.textContent = '↓';
    const label = document.createElement('span');
    label.textContent = name;
    const action = document.createElement('small');
    action.textContent = 'Baixar';
    link.append(icon, label, action);
    return link;
  }));
}

async function refreshCatalog() {
  if (catalogRefreshing || elements.filesPanel.hidden) return;
  catalogRefreshing = true;
  try {
    const response = await fetch('/api/files?name=catalogo.txt', { cache: 'no-store' });
    if (response.status === 404) {
      renderCatalog([]);
      return;
    }
    if (!response.ok) throw new Error(`Falha HTTP ${response.status}`);
    const names = (await response.text()).split(/\r?\n/).filter(Boolean);
    renderCatalog(names);
  } catch (error) {
    elements.catalogList.innerHTML = `<p class="global-error">Não foi possível carregar o catálogo: ${escapeHtml(error.message)}</p>`;
  } finally {
    catalogRefreshing = false;
  }
}

function escapeHtml(value) {
  const element = document.createElement('span');
  element.textContent = value;
  return element.innerHTML;
}

function fileAsBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.addEventListener('load', () => resolve(String(reader.result).split(',', 2)[1] || ''));
    reader.addEventListener('error', () => reject(reader.error || new Error('Não foi possível ler o arquivo')));
    reader.readAsDataURL(file);
  });
}

async function refresh() {
  if (refreshing) return;
  refreshing = true;
  try {
    const response = await fetch('/api/state', { cache: 'no-store' });
    if (!response.ok) throw new Error(`Falha HTTP ${response.status}`);
    render(await response.json());
    refreshCatalog();
    elements.connectionDot.classList.add('online');
    elements.connectionLabel.textContent = 'Nó acessível';
    elements.globalError.hidden = true;
  } catch (error) {
    elements.connectionDot.classList.remove('online');
    elements.connectionLabel.textContent = 'Sem conexão';
    elements.globalError.textContent = `Não foi possível consultar o nó: ${error.message}`;
    elements.globalError.hidden = false;
  } finally {
    refreshing = false;
  }
}

document.querySelectorAll('input[name="mode"]').forEach((input) => {
  input.addEventListener('change', () => {
    const existing = input.value === 'existing' && input.checked;
    if (!input.checked) return;
    elements.bootstrapFields.hidden = !existing;
    elements.joinButton.textContent = existing ? 'Entrar no anel' : 'Criar anel';
  });
});

elements.joinForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  elements.joinButton.disabled = true;
  elements.joinMessage.className = 'form-message';
  elements.joinMessage.textContent = 'Conectando…';
  const existing = document.querySelector('input[name="mode"]:checked').value === 'existing';
  const bootstrap = existing ? {
    id: Number(document.querySelector('#bootstrap-id').value),
    host: document.querySelector('#bootstrap-host').value.trim(),
    port: Number(document.querySelector('#bootstrap-port').value)
  } : null;

  try {
    const response = await fetch('/join', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ bootstrap })
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || 'Não foi possível entrar no anel');
    elements.joinMessage.textContent = 'Nó conectado com sucesso.';
    render(result);
  } catch (error) {
    elements.joinMessage.className = 'form-message error';
    elements.joinMessage.textContent = error.message;
  } finally {
    elements.joinButton.disabled = false;
  }
});

elements.refreshButton.addEventListener('click', refresh);
elements.filesRefreshButton.addEventListener('click', refreshCatalog);
elements.uploadFile.addEventListener('change', () => {
  elements.selectedFile.textContent = elements.uploadFile.files[0]?.name
    || 'Nenhum arquivo selecionado';
});
elements.uploadForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const file = elements.uploadFile.files[0];
  if (!file) return;

  elements.uploadButton.disabled = true;
  elements.uploadMessage.className = 'form-message';
  elements.uploadMessage.textContent = 'Enviando…';
  try {
    const response = await fetch('/api/files', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        name: file.name,
        content: await fileAsBase64(file),
        encoding: 'base64'
      })
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || 'Não foi possível enviar o arquivo');
    elements.uploadMessage.textContent = result.replicaNode
      ? `Arquivo armazenado pelo nó ${result.node.id} (hash ${result.hashId}); réplica no nó ${result.replicaNode.id}.`
      : `Arquivo armazenado pelo nó ${result.node.id} (hash ${result.hashId}).`;
    elements.uploadForm.reset();
    elements.selectedFile.textContent = 'Nenhum arquivo selecionado';
    await refreshCatalog();
  } catch (error) {
    elements.uploadMessage.className = 'form-message error';
    elements.uploadMessage.textContent = error.message;
  } finally {
    elements.uploadButton.disabled = false;
  }
});
refresh();
setInterval(() => {
  refresh();
  refreshCatalog();
}, 5000);
