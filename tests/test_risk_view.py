import pytest
from playwright.sync_api import sync_playwright, expect

@pytest.fixture(scope="module")
def browser_context():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        yield context
        browser.close()

def test_risk_view_highlights_correct_farm(browser_context):
    page = browser_context.new_page()
    try:
        # Navigate to the app
        page.goto("http://localhost:8000", wait_until="networkidle")

        # Mock application state
        page.evaluate("""
            window.App.state.currentUser = {
                uid: 'test-user',
                companyId: 'test-company',
                role: 'admin',
                permissions: {
                    monitoramentoAereo: true
                }
            };
            window.App.state.companies = [{id: 'test-company', subscribedModules: ['monitoramentoAereo'] }];
            window.App.state.globalConfigs = { monitoramentoAereo: true };
            window.App.state.fazendas = [
                { id: '1', code: '123', name: 'Fazenda Risco Alto', companyId: 'test-company', talhoes: [] },
                { id: '2', code: '456', name: 'Fazenda Risco Baixo', companyId: 'test-company', talhoes: [] }
            ];
            window.App.state.armadilhas = [
                // 4 traps for Fazenda 123, 3 with high count -> 75% risk
                { id: 't1', fazendaCode: '123', status: 'Coletada', contagemMariposas: 10, dataColeta: new Date(), companyId: 'test-company' },
                { id: 't2', fazendaCode: '123', status: 'Coletada', contagemMariposas: 10, dataColeta: new Date(), companyId: 'test-company' },
                { id: 't3', fazendaCode: '123', status: 'Coletada', contagemMariposas: 10, dataColeta: new Date(), companyId: 'test-company' },
                { id: 't4', fazendaCode: '123', status: 'Coletada', contagemMariposas: 2, dataColeta: new Date(), companyId: 'test-company' },
                 // 4 traps for Fazenda 456, 1 with high count -> 25% risk
                { id: 't5', fazendaCode: '456', status: 'Coletada', contagemMariposas: 10, dataColeta: new Date(), companyId: 'test-company' },
                { id: 't6', fazendaCode: '456', status: 'Coletada', contagemMariposas: 2, dataColeta: new Date(), companyId: 'test-company' },
                { id: 't7', fazendaCode: '456', status: 'Coletada', contagemMariposas: 2, dataColeta: new Date(), companyId: 'test-company' },
                { id: 't8', fazendaCode: '456', status: 'Coletada', contagemMariposas: 2, dataColeta: new Date(), companyId: 'test-company' }
            ];
            window.App.state.geoJsonData = {
                "type": "FeatureCollection",
                "features": [
                    { "type": "Feature", "id": 1, "properties": { "FUNDO_AGR": "123" }, "geometry": { "type": "Polygon", "coordinates": [[[-48.0, -21.0], [-48.1, -21.0], [-48.1, -21.1], [-48.0, -21.1], [-48.0, -21.0]]] } },
                    { "type": "Feature", "id": 2, "properties": { "FUNDO_AGR": "456" }, "geometry": { "type": "Polygon", "coordinates": [[[-48.2, -21.2], [-48.3, -21.2], [-48.3, -21.3], [-48.2, -21.3], [-48.2, -21.2]]] } }
                ]
            };

            // Show the app screen and map
            window.App.ui.showAppScreen();
            window.App.ui.showTab('monitoramentoAereo');

            // Replace the real map object after it has been initialized
            window.App.state.mapboxMap = {
                _paint: {},
                _sources: {
                    'talhoes-source': {
                         _data: window.App.state.geoJsonData,
                        setData: function(data) { this._data = data; }
                    }
                },
                featureStates: {},
                getLayer: function(id) { return this._layers && this._layers[id]; },
                getSource: function(id) { return this._sources[id]; },
                setPaintProperty: function(layer, prop, value) { this._paint[layer] = this._paint[layer] || {}; this._paint[layer][prop] = value; },
                setFeatureState: function(feature, state) {
                    const key = `${feature.source}-${feature.id}`;
                    this.featureStates[key] = { ...this.featureStates[key], ...state };
                },
                riskFarmFeatureIds: [],
                isStyleLoaded: () => true, // Mock style loaded
                on: () => {}, // Mock event listener
                addSource: () => {},
                addLayer: (layer) => {
                    if(!this._layers) this._layers = {};
                    this._layers[layer.id] = layer;
                },
                getCanvas: () => ({style: {cursor: ''}}),
                querySourceFeatures: function(sourceId, filter) {
                    return window.App.state.geoJsonData.features;
                },
                on: () => {},
                flyTo: () => {},
                fitBounds: () => {}
            };
        """)

        # Trigger the risk view toggle
        page.click("#btnToggleRiskView")

        # Check the result from the mock map object
        feature_state = page.evaluate("() => window.App.state.mapboxMap.featureStates")

        # Farm with code '123' (ID 1) should be high risk
        assert feature_state is not None
        assert 'talhoes-source-1' in feature_state
        assert feature_state['talhoes-source-1']['risk'] is True

        # Farm with code '456' (ID 2) should not be high risk
        assert 'talhoes-source-2' not in feature_state or 'risk' not in feature_state['talhoes-source-2'] or feature_state['talhoes-source-2']['risk'] is False

    finally:
        page.close()
