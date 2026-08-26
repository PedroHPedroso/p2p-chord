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
  fileTrace: document.querySelector('#file-trace'),
  traceClose: document.querySelector('#trace-close'),
  traceName: document.querySelector('#trace-name'),
  traceHash: document.querySelector('#trace-hash'),
  traceUploader: document.querySelector('#trace-uploader'),
  traceUploadedAt: document.querySelector('#trace-uploaded-at'),
  tracePrimary: document.querySelector('#trace-primary'),
  tracePrimaryAddress: document.querySelector('#trace-primary-address'),
  traceReplicaCount: document.querySelector('#trace-replica-count'),
  traceLocations: document.querySelector('#trace-locations'),
  traceEvents: document.querySelector('#trace-events')
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
  renderNode(elements.predecessorId, elements.predecessorAddress, state.predecessor);
  renderNode(elements.successorId, elements.successorAddress, state.successor);
  renderTable(state.fingerTable);
  renderRing(state);
  elements.lastUpdate.textContent = `Atualizado às ${new Date().toLocaleTimeString('pt-BR')}`;
}

function renderCatalog(names) {
  if (names.length === 0) {
    elements.catalogList.innerHTML = '<p class="empty-row">Nenhum arquivo foi inserido na rede.</p>';
    return;
  }

  elements.catalogList.replaceChildren(...names.map((name) => {
    const card = document.createElement('article');
    card.className = 'catalog-file';

    const icon = document.createElement('span');
    icon.className = 'file-icon';
    icon.textContent = '↓';
    const details = document.createElement('span');
    details.className = 'catalog-file-details';
    const label = document.createElement('strong');
    label.textContent = name;
    const locations = document.createElement('small');
    locations.className = 'file-locations';
    locations.textContent = 'Consultando cópias…';
    details.append(label, locations);
    const actions = document.createElement('span');
    actions.className = 'catalog-file-actions';
    const track = document.createElement('button');
    track.type = 'button';
    track.className = 'text-action';
    track.textContent = 'Rastrear';
    track.addEventListener('click', () => showFileTrace(name));
    const download = document.createElement('a');
    download.href = `/api/files?name=${encodeURIComponent(name)}`;
    download.download = name;
    download.textContent = 'Baixar';
    actions.append(track, download);
    card.append(icon, details, actions);
    fillFileLocations(name, locations);
    return card;
  }));
}

async function showFileTrace(name) {
  elements.fileTrace.hidden = false;
  elements.traceName.textContent = name;
  elements.traceHash.textContent = 'Consultando a rede…';
  elements.traceLocations.innerHTML = '<p class="muted">Localizando cópias…</p>';
  elements.traceEvents.replaceChildren();
  elements.fileTrace.scrollIntoView({ behavior: 'smooth', block: 'nearest' });

  try {
    const response = await fetch(`/api/files/locations?name=${encodeURIComponent(name)}`, {
      cache: 'no-store'
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || `Falha HTTP ${response.status}`);
    renderFileTrace(result);
  } catch (error) {
    elements.traceHash.textContent = error.message;
    elements.traceLocations.innerHTML = '<p class="global-error">Não foi possível rastrear o arquivo.</p>';
  }
}

function renderFileTrace(result) {
  const uploader = result.uploadedBy;
  elements.traceHash.textContent = `Hash Chord: ${result.hashId}`;
  elements.traceUploader.textContent = uploader ? `Nó ${uploader.id}` : 'Não registrado';
  elements.traceUploadedAt.textContent = result.uploadedAt
    ? `em ${formatDate(result.uploadedAt)}`
    : 'Arquivo importado de uma versão anterior';
  elements.tracePrimary.textContent = result.primary ? `Nó ${result.primary.id}` : '—';
  elements.tracePrimaryAddress.textContent = result.primary
    ? address(result.primary)
    : 'Nenhum primário online';
  elements.traceReplicaCount.textContent = `${result.replicas.length} / ${result.replicaLimit}`;

  if (!result.locations.length) {
    elements.traceLocations.innerHTML = '<p class="muted">Nenhuma cópia online encontrada.</p>';
  } else {
    elements.traceLocations.replaceChildren(...result.locations.map((location) => {
      const row = document.createElement('div');
      row.className = `trace-location ${location.role}`;
      const badge = document.createElement('span');
      badge.className = 'node-badge';
      badge.textContent = location.id;
      const details = document.createElement('span');
      const title = document.createElement('strong');
      title.textContent = location.role === 'primary' ? 'Primário' : 'Réplica';
      const nodeAddress = document.createElement('small');
      nodeAddress.textContent = address(location);
      details.append(title, nodeAddress);
      row.append(badge, details);
      return row;
    }));
  }

  const events = result.events || [];
  if (!events.length) {
    const item = document.createElement('li');
    item.textContent = 'Não há eventos registrados para este arquivo.';
    elements.traceEvents.replaceChildren(item);
    return;
  }
  elements.traceEvents.replaceChildren(...events.map((event) => {
    const item = document.createElement('li');
    const label = document.createElement('strong');
    if (event.type === 'upload') {
      label.textContent = `Nó ${event.node?.id ?? '?'} inseriu o arquivo`;
    } else if (event.type === 'primary_transferred') {
      label.textContent = `Primário transferido do nó ${event.fromNode?.id ?? '?'} para o nó ${event.node?.id ?? '?'}`;
    } else {
      label.textContent = 'Arquivo recuperado do armazenamento anterior';
    }
    const time = document.createElement('small');
    time.textContent = event.timestamp && !event.timestamp.startsWith('1970-')
      ? formatDate(event.timestamp)
      : 'data não disponível';
    item.append(label, time);
    return item;
  }));
}

function formatDate(value) {
  return new Date(value).toLocaleString('pt-BR');
}

async function fillFileLocations(name, target) {
  try {
    const response = await fetch(`/api/files/locations?name=${encodeURIComponent(name)}`, {
      cache: 'no-store'
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || `Falha HTTP ${response.status}`);
    const primary = result.locations.find((location) => location.role === 'primary');
    const replicas = result.locations.filter((location) => location.role === 'replica');
    const parts = [];
    if (primary) parts.push(`Primário: nó ${primary.id}`);
    if (replicas.length) parts.push(`Réplicas: ${replicas.map((node) => `nó ${node.id}`).join(', ')}`);
    target.textContent = parts.join(' · ') || 'Nenhuma cópia online encontrada';
  } catch {
    target.textContent = 'Localizações indisponíveis';
  }
}

async function refreshCatalog() {
  if (catalogRefreshing || elements.filesPanel.hidden) return;
  catalogRefreshing = true;
  try {
    const response = await fetch('/api/catalog', { cache: 'no-store' });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || `Falha HTTP ${response.status}`);
    renderCatalog(result.names || []);
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
elements.traceClose.addEventListener('click', () => {
  elements.fileTrace.hidden = true;
});
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
    const primary = `nó ${result.primary.id} (${address(result.primary)})`;
    const replicas = result.replicas.length
      ? result.replicas.map((node) => `nó ${node.id} (${address(node)})`).join(', ')
      : 'nenhuma cópia confirmada';
    elements.uploadMessage.textContent = `Hash ${result.hashId}. Primário: ${primary}. Réplicas: ${replicas}.`;
    elements.uploadForm.reset();
    elements.selectedFile.textContent = 'Nenhum arquivo selecionado';
    await refreshCatalog();
    await showFileTrace(result.name);
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
