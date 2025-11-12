
import { db, collection, query, where, onSnapshot, serverTimestamp } from './firebase.js';
import { App } from './app.js';

App.ordemServico = {
    initMap() {
        if (App.state.osMapLoaded) return; // Já inicializado
        if (!App.elements.ordemServico.mapContainer) return;
        if (typeof mapboxgl === 'undefined') {
            App.ui.showAlert("Mapbox GL JS não está carregado.", "error");
            return;
        }
        try {
            App.state.osMap = new mapboxgl.Map({
                container: App.elements.ordemServico.mapContainer,
                style: 'mapbox://styles/mapbox/satellite-streets-v12',
                center: [-48.45, -21.17], // Centro padrão
                zoom: 12,
                attributionControl: false
            });

            App.state.osMap.on('load', () => {
                App.state.osMapLoaded = true;
                console.log("Mapa da O.S. inicializado.");
                // Se uma fazenda já estiver selecionada (ex: rascunho), carregue-a
                const fazendaId = App.elements.ordemServico.fazenda.value;
                if (fazendaId) {
                    this.loadFazendaOnMap(fazendaId);
                }
            });

        } catch (e) {
            console.error("Erro ao inicializar o mapa da O.S.:", e);
            App.ui.showAlert("Não foi possível carregar o mapa da O.S.", "error");
        }
    },

    destroyMap() {
        if (App.state.osMap) {
            App.state.osMap.remove();
            App.state.osMap = null;
            App.state.osMapLoaded = false;
            if (App.state.osMapClickListener) {
                // Remove o listener de clique se existir um mapa principal
                if (App.state.mapboxMap) {
                    App.state.mapboxMap.off('click', 'talhoes-layer', App.state.osMapClickListener);
                }
                App.state.osMapClickListener = null;
            }
            console.log("Mapa da O.S. destruído.");
        }
        this.clearOSForm();
    },

    clearOSForm() {
        App.state.osSelectedTalhaoIds.clear();
        App.state.osCurrentFazendaId = null;
        const els = App.elements.ordemServico;
        els.form.reset();
        els.data.value = new Date().toISOString().split('T')[0];
        this.updateSelectedTalhoesUI();
    },

    loadFazendaOnMap(fazendaId) {
        if (!App.state.osMapLoaded) return;
        if (!fazendaId) {
            // Limpa o mapa se nenhuma fazenda for selecionada
            this.clearMapLayers();
            return;
        }

        const fazenda = App.state.fazendas.find(f => f.id === fazendaId);
        if (!fazenda) {
            App.ui.showAlert("Fazenda não encontrada.", "error");
            return;
        }

        // Limpa seleções e camadas anteriores
        this.clearMapLayers();
        App.state.osSelectedTalhaoIds.clear();
        App.state.osCurrentFazendaId = fazendaId;
        this.updateSelectedTalhoesUI();

        const map = App.state.osMap;
        const sourceId = 'os-talhoes-source';

        // Filtra o GeoJSON principal (App.state.geoJsonData)
        if (!App.state.geoJsonData) {
            App.ui.showAlert("Dados de shapefile (GeoJSON) não estão carregados no App.state.", "error");
            return;
        }

        // Filtra as features que pertencem a esta fazenda pelo CÓDIGO (FUNDO_AGR)
        const farmFeatures = App.state.geoJsonData.features.filter(feature => {
            const fundoAgricola = App.mapModule._findProp(feature, ['FUNDO_AGR']);
            return String(fundoAgricola).trim() === String(fazenda.code).trim();
        });

        if (farmFeatures.length === 0) {
            App.ui.showAlert("Nenhum talhão encontrado no shapefile para esta fazenda.", "warning");
            return;
        }

        const farmGeoJson = {
            type: 'FeatureCollection',
            features: farmFeatures
        };

        map.addSource(sourceId, {
            type: 'geojson',
            data: farmGeoJson,
            generateId: true
        });

        // Camada de preenchimento (para pintar)
        map.addLayer({
            id: 'os-talhoes-fill',
            type: 'fill',
            source: sourceId,
            paint: {
                'fill-color': [
                    'case',
                    ['boolean', ['feature-state', 'selected'], false],
                    '#2e7d32', // Cor primária (Pintado)
                    '#FFFFFF' // Cor padrão (Branco)
                ],
                'fill-opacity': [
                    'case',
                    ['boolean', ['feature-state', 'selected'], false],
                    0.8, // Opacidade quando selecionado
                    0.4 // Opacidade padrão
                ]
            }
        });

        // Camada de borda
        map.addLayer({
            id: 'os-talhoes-border',
            type: 'line',
            source: sourceId,
            paint: {
                'line-color': '#333333',
                'line-width': 2
            }
        });

        // Camada de rótulos
        map.addLayer({
            id: 'os-talhoes-labels',
            type: 'symbol',
            source: sourceId,
            layout: {
                'text-field': ['get', 'AGV_TALHAO'],
                'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
                'text-size': 12,
                'text-allow-overlap': true
            },
            paint: {
                'text-color': '#000000',
                'text-halo-color': '#FFFFFF',
                'text-halo-width': 1
            }
        });

        // Zoom para a área dos talhões
        const bounds = turf.bbox(farmGeoJson);
        map.fitBounds(bounds, {
            padding: 40
        });

        // Adiciona o listener de clique
        if (App.state.osMapClickListener) {
            map.off('click', 'os-talhoes-fill', App.state.osMapClickListener);
        }
        App.state.osMapClickListener = (e) => this.onMapClick(e);
        map.on('click', 'os-talhoes-fill', App.state.osMapClickListener);
    },

    clearMapLayers() {
        const map = App.state.osMap;
        if (!map) return;

        if (App.state.osMapClickListener) {
            map.off('click', 'os-talhoes-fill', App.state.osMapClickListener);
            App.state.osMapClickListener = null;
        }

        const layers = ['os-talhoes-fill', 'os-talhoes-border', 'os-talhoes-labels'];
        layers.forEach(id => {
            if (map.getLayer(id)) {
                map.removeLayer(id);
            }
        });

        if (map.getSource('os-talhoes-source')) {
            map.removeSource('os-talhoes-source');
        }
    },

    onMapClick(e) {
        if (!e.features || e.features.length === 0) return;

        const map = App.state.osMap;
        const feature = e.features[0];
        const talhaoId = feature.id; // ID gerado pelo Mapbox
        const talhaoNome = App.mapModule._findProp(feature, ['CD_TALHAO', 'COD_TALHAO', 'TALHAO']);

        if (!talhaoNome || talhaoNome === 'Não identificado') {
            App.ui.showAlert("Talhão sem nome/ID no shapefile. Não pode ser selecionado.", "warning");
            return;
        }

        const isSelected = App.state.osSelectedTalhaoIds.has(talhaoNome);

        if (isSelected) {
            App.state.osSelectedTalhaoIds.delete(talhaoNome);
            map.setFeatureState({
                source: 'os-talhoes-source',
                id: talhaoId
            }, {
                selected: false
            });
        } else {
            App.state.osSelectedTalhaoIds.add(talhaoNome);
            map.setFeatureState({
                source: 'os-talhoes-source',
                id: talhaoId
            }, {
                selected: true
            });
        }

        this.updateSelectedTalhoesUI();
    },

    updateSelectedTalhoesUI() {
        const listEl = App.elements.ordemServico.selectedTalhoesList;
        listEl.innerHTML = '';
        if (App.state.osSelectedTalhaoIds.size === 0) {
            listEl.innerHTML = '<span style="color: var(--color-text-light);">Nenhum talhão selecionado.</span>';
            return;
        }

        App.state.osSelectedTalhaoIds.forEach(talhaoNome => {
            const tag = document.createElement('span');
            tag.className = 'status-badge'; // Reutilizando um estilo
            tag.style.backgroundColor = 'var(--color-primary-light)';
            tag.style.color = 'var(--color-primary-dark)';
            tag.style.fontWeight = '500';
            tag.textContent = talhaoNome;
            listEl.appendChild(tag);
        });
    },

    async saveOS() {
        const els = App.elements.ordemServico;
        const entryId = els.entryId.value; // Para futuras edições

        if (!App.ui.validateFields([els.data.id, els.tipo.id, els.responsavel.id, els.fazenda.id])) {
            App.ui.showAlert("Preencha todos os detalhes da O.S.", "error");
            return;
        }
        if (App.state.osSelectedTalhaoIds.size === 0) {
            App.ui.showAlert("Selecione pelo menos um talhão no mapa.", "error");
            return;
        }

        const responsavelEl = els.responsavel;
        const responsavelNome = responsavelEl.options[responsavelEl.selectedIndex] ? .text || 'N/A';

        const fazendaEl = els.fazenda;
        const fazendaNome = fazendaEl.options[fazendaEl.selectedIndex] ? .text || 'N/A';

        const osData = {
            data: els.data.value,
            tipoServico: els.tipo.value,
            idResponsavel: els.responsavel.value,
            nomeResponsavel: responsavelNome,
            idFazenda: els.fazenda.value,
            nomeFazenda: fazendaNome,
            talhoesSelecionados: Array.from(App.state.osSelectedTalhaoIds),
            status: 'Pendente',
            companyId: App.state.currentUser.companyId,
            usuarioCriacao: App.state.currentUser.username,
        };

        const confirmationMessage = entryId ? "Atualizar esta Ordem de Serviço?" : "Salvar esta nova Ordem de Serviço?";

        App.ui.showConfirmationModal(confirmationMessage, async () => {
            App.ui.setLoading(true, "Salvando O.S....");
            try {
                if (entryId) {
                    // Lógica de Atualização (não implementada neste rascunho)
                    // await App.data.updateDocument('ordensDeServico', entryId, osData);
                    // App.ui.showAlert("O.S. atualizada com sucesso!");
                } else {
                    osData.createdAt = serverTimestamp();
                    // Gera um ID amigável
                    const osId = `OS-${Date.now().toString().slice(-6)}`;
                    osData.osId = osId;

                    // Salva usando o ID amigável como Doc ID
                    await App.data.setDocument('ordensDeServico', osId, osData);
                    App.ui.showAlert(`Ordem de Serviço ${osId} salva com sucesso!`);
                }
                this.clearOSForm();
                App.ui.showTab('dashboard'); // Volta para o dashboard
            } catch (e) {
                App.ui.showAlert("Erro ao salvar O.S.", "error");
                console.error(e);
            } finally {
                App.ui.setLoading(false);
            }
        });
    },

    renderRelatorioLista() {
        const els = App.elements.relatorioOrdemServico;
        const listaEl = els.lista;

        const filtroInicio = els.filtroInicio.value;
        const filtroFim = els.filtroFim.value;
        const filtroOS = els.filtroOS.value.trim().toUpperCase();

        let dadosFiltrados = App.state.ordensDeServico;

        if (filtroInicio) {
            dadosFiltrados = dadosFiltrados.filter(os => os.data >= filtroInicio);
        }
        if (filtroFim) {
            dadosFiltrados = dadosFiltrados.filter(os => os.data <= filtroFim);
        }
        if (filtroOS) {
            dadosFiltrados = dadosFiltrados.filter(os => os.id.toUpperCase().includes(filtroOS));
        }

        // Ordena pela data de criação
        dadosFiltrados.sort((a, b) => {
            const dateA = a.createdAt ? .toDate ? a.createdAt.toDate() : new Date(a.data);
            const dateB = b.createdAt ? .toDate ? b.createdAt.toDate() : new Date(b.data);
            return dateB - dateA;
        });

        if (dadosFiltrados.length === 0) {
            listaEl.innerHTML = '<p style="text-align:center; padding: 20px; color: var(--color-text-light);">Nenhuma Ordem de Serviço encontrada para os filtros selecionados.</p>';
            return;
        }

        listaEl.innerHTML = dadosFiltrados.map(os => {
            const dataOS = new Date(os.data + 'T03:00:00Z').toLocaleDateString('pt-BR');
            return `
            <div class="plano-card" style="border-left-color: var(--color-purple);">
                <div class="plano-header">
                    <span class="plano-title"><i class="fas fa-clipboard-list"></i> ${os.id} (${os.tipoServico})</span>
                    <span class="plano-status" style="background-color: ${os.status === 'Pendente' ? 'var(--color-warning)' : 'var(--color-success)'};">
                        ${os.status}
                    </span>
                </div>
                <div class="plano-details" style="grid-template-columns: 1fr 1fr;">
                    <div><i class="fas fa-calendar-day"></i> Data: ${dataOS}</div>
                    <div><i class="fas fa-user-check"></i> Responsável: ${os.nomeResponsavel}</div>
                    <div><i class="fas fa-tractor"></i> Fazenda: ${os.nomeFazenda}</div>
                    <div><i class="fas fa-th-large"></i> Talhões: ${os.talhoesSelecionados.join(', ')}</div>
                </div>
                <div class="plano-actions">
                    <button class="save" data-action="gerar-pdf" data-id="${os.id}" style="max-width: 200px; margin-left: 0;">
                        <i class="fas fa-file-pdf"></i> Gerar PDF do Mapa
                    </button>
                </div>
            </div>
        `;
        }).join('');
    },

    handleReportClick(e) {
        const button = e.target.closest('button[data-action="gerar-pdf"]');
        if (button) {
            const osId = button.dataset.id;
            App.reports.generateOrdemServicoPDF(osId);
        }
    }
};
