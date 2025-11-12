const { test, expect } = require('@playwright/test');

test.describe('Farm Risk Alert Feature', () => {
    test('should highlight at-risk farms, hide others, and show risk percentage on click', async ({ page }) => {
        await page.goto('http://localhost:8000/index.html');

        // 1. Wait for the app to be ready
        await page.waitForFunction(() => window.App !== undefined);
        await expect(page.locator('#loginScreen')).toBeVisible();

        const companyId = 'company-123';
        const atRiskFarmCode = '100';
        const notAtRiskFarmCode = '200';

        // 2. Mock the entire App.state to simulate a logged-in user and specific data
        await page.evaluate((args) => {
            const { companyId, atRiskFarmCode, notAtRiskFarmCode } = args;

            // Mock user and company
            window.App.state.currentUser = {
                uid: 'test-user-id',
                username: 'Test User',
                email: 'test@example.com',
                role: 'admin',
                companyId: companyId,
                permissions: { dashboard: true, monitoramentoAereo: true }
            };
            window.App.state.companies = [{ id: companyId, name: 'Test Company', subscribedModules: ['monitoramentoAereo'] }];

            // Mock global feature flags
            window.App.state.globalConfigs = {
                monitoramentoAereo: true,
            };

            // Mock farm data
            window.App.state.fazendas = [
                { id: 'farm-A', code: atRiskFarmCode, name: 'FARM AT RISK', companyId: companyId, talhoes: [] },
                { id: 'farm-B', code: notAtRiskFarmCode, name: 'FARM NOT AT RISK', companyId: companyId, talhoes: [] },
            ];

            // Mock trap and collection data
            const lastInstallDate = new Date();
            lastInstallDate.setDate(lastInstallDate.getDate() - 5); // Installed 5 days ago

            const oldInstallDate = new Date();
            oldInstallDate.setDate(oldInstallDate.getDate() - 20);


            window.App.state.armadilhas = [
                // Traps for the at-risk farm - Using the new `fazendaCode` for matching
                { id: 'trap-1', fazendaCode: atRiskFarmCode, fazendaNome: 'FARM AT RISK', companyId: companyId, status: 'Coletada', dataInstalacao: oldInstallDate, dataColeta: new Date(), contagemMariposas: 2 },
                { id: 'trap-2', fazendaCode: atRiskFarmCode, fazendaNome: 'FARM AT RISK', companyId: companyId, status: 'Coletada', dataInstalacao: lastInstallDate, dataColeta: new Date(), contagemMariposas: 10 }, // High count after recent install

                // Trap for the safe farm - Using legacy `fazendaNome` for matching to ensure fallback works
                { id: 'trap-3', fazendaNome: 'FARM NOT AT RISK', companyId: companyId, status: 'Coletada', dataInstalacao: lastInstallDate, dataColeta: new Date(), contagemMariposas: 3 },
            ];

            // Mock GeoJSON data for map visualization
            window.App.state.geoJsonData = {
                type: 'FeatureCollection',
                features: [
                    {
                        type: 'Feature',
                        id: 1, // Mapbox generates numeric IDs
                        geometry: { type: 'Polygon', coordinates: [[[-48, -21], [-48, -21.1], [-48.1, -21.1], [-48.1, -21], [-48, -21]]] },
                        properties: { FUNDO_AGR: atRiskFarmCode, NM_IMOVEL: 'FARM AT RISK' }
                    },
                    {
                        type: 'Feature',
                        id: 2,
                        geometry: { type: 'Polygon', coordinates: [[[-49, -22], [-49, -22.1], [-49.1, -22.1], [-49.1, -22], [-49, -22]]] },
                        properties: { FUNDO_AGR: notAtRiskFarmCode, NM_IMOVEL: 'FARM NOT AT RISK' }
                    }
                ]
            };
        }, { companyId, atRiskFarmCode, notAtRiskFarmCode });

        // 3. Manually trigger the app to show the main screen
        await page.evaluate(() => window.App.ui.showAppScreen());
        await expect(page.locator('#appScreen')).toBeVisible();

        // 4. Navigate to the Monitoring tab
        await page.locator('#btnToggleMenu').click(); // Open the main menu
        await page.locator('nav').getByRole('button', { name: 'Monitoramento Aéreo' }).click();
        await expect(page.locator('#monitoramentoAereo-container')).toBeVisible();

        // Wait for the map to fully load and render the shapes
        await page.waitForFunction(() => window.App.state.mapboxMap && window.App.state.mapboxMap.isStyleLoaded());
        await page.waitForTimeout(1000); // Allow time for layers to be added

        // 5. Activate the risk view
        const riskButton = page.locator('#btnToggleRiskView');
        await expect(riskButton).toBeVisible();
        await riskButton.click();

        // Wait for the calculation to finish
        await page.waitForFunction(() => window.App.state.riskViewActive === true);
        await page.waitForTimeout(500); // Give it a moment to apply styles

        // 6. Verify visual state of the map features
        const atRiskFeatureState = await page.evaluate(() => window.App.state.mapboxMap.getFeatureState({ source: 'talhoes-source', id: 1 }));
        expect(atRiskFeatureState.risk).toBe(true);

        const notAtRiskFeatureState = await page.evaluate(() => window.App.state.mapboxMap.getFeatureState({ source: 'talhoes-source', id: 2 }));
        expect(notAtRiskFeatureState.risk).toBe(undefined);

        // Verify paint properties for isolation view
        const fillOpacity = await page.evaluate(() => window.App.state.mapboxMap.getPaintProperty('talhoes-layer', 'fill-opacity'));
        // Check if the 'case' expression for risk is correctly set
        expect(fillOpacity[1][1][1]).toEqual('risk'); // ['boolean', ['feature-state', 'risk'], false]

        const lineOpacity = await page.evaluate(() => window.App.state.mapboxMap.getPaintProperty('talhoes-border-layer', 'line-opacity'));
        expect(lineOpacity[1][1][1]).toEqual('risk');

        // 7. Simulate click on the at-risk farm and verify the popup
        await page.evaluate(() => {
            const map = window.App.state.mapboxMap;
            const feature = window.App.state.geoJsonData.features[0]; // The at-risk one
            // Simulate the click event logic
            App.mapModule.showTalhaoInfo(feature, 50.0);
        });

        // 8. Check that the info box is visible and contains the risk percentage
        const infoBox = page.locator('#talhao-info-box');
        await expect(infoBox).toBeVisible();
        await expect(infoBox).toContainText('Risco de Aplicação');
        await expect(infoBox).toContainText('50.00%');
        await expect(infoBox).toContainText('FARM AT RISK');

        console.log('Test successfully verified risk highlighting, view isolation, and percentage display.');
    });
});
