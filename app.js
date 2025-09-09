const plantios = [];
const colheitas = [];

function showTab(id) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById(id).classList.add('active');
}

function scrollToSection(id) {
  const el = document.getElementById(id);
  if (el) {
    el.scrollIntoView({ behavior: 'smooth' });
  }
}

document.getElementById('plantioForm').addEventListener('submit', e => {
  e.preventDefault();
  const data = Object.fromEntries(new FormData(e.target));
  plantios.push({ talhao: data.talhao, variedade: data.variedade, area: parseFloat(data.area), data: data.data });
  renderPlantio();
  e.target.reset();
});

function renderPlantio() {
  const tbody = document.querySelector('#plantioTable tbody');
  tbody.innerHTML = '';
  plantios.forEach(p => {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${p.talhao}</td><td>${p.variedade}</td><td>${p.area.toFixed(2)}</td><td>${p.data}</td>`;
    tbody.appendChild(tr);
  });
}

document.getElementById('colheitaForm').addEventListener('submit', e => {
  e.preventDefault();
  const data = Object.fromEntries(new FormData(e.target));
  const ordem = `OC-${colheitas.length + 1}`;
  colheitas.push({ ordem, talhao: data.talhao, data: data.data, status: 'Pendente' });
  renderColheita();
  e.target.reset();
});

function renderColheita() {
  const tbody = document.querySelector('#colheitaTable tbody');
  tbody.innerHTML = '';
  colheitas.forEach((c, idx) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${c.ordem}</td><td>${c.talhao}</td><td>${c.data}</td><td>${c.status}</td>` +
      `<td><button class="btn btn-sm btn-outline-success" onclick="concluirColheita(${idx})">Concluir</button></td>`;
    tbody.appendChild(tr);
  });
}

function concluirColheita(index) {
  colheitas[index].status = 'Concluído';
  renderColheita();
}

function gerarRelatorios() {
  gerarCensoVarietal();
  gerarFaltaColher();
}

function gerarCensoVarietal() {
  const resumo = {};
  plantios.forEach(p => {
    resumo[p.variedade] = (resumo[p.variedade] || 0) + p.area;
  });
  const tbody = document.querySelector('#censoTable tbody');
  tbody.innerHTML = '';
  Object.entries(resumo).forEach(([variedade, area]) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${variedade}</td><td>${area.toFixed(2)}</td>`;
    tbody.appendChild(tr);
  });
}

function gerarFaltaColher() {
  const plantioTalhoes = plantios.map(p => p.talhao);
  const colhidos = new Set(colheitas.filter(c => c.status === 'Concluído').map(c => c.talhao));
  const pendentes = plantioTalhoes.filter(t => !colhidos.has(t));
  const ul = document.getElementById('faltaColher');
  ul.innerHTML = '';
  pendentes.forEach(t => {
    const li = document.createElement('li');
    li.textContent = t;
    li.className = 'list-group-item';
    ul.appendChild(li);
  });
}
