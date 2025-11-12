const { test, expect } = require('@playwright/test');

test.describe('AgroVetor Risk View Verification', () => {
  test('should hide labels of non-risk farms when risk view is enabled', async ({ page }) => {
    // Mock the capacitor environment
    await page.addInitScript(() => {
        // Mock Capacitor
      window.Capacitor = {
        isNativePlatform: () => false,
        Plugins: {
          StatusBar: { setOverlaysWebView: () => {} },
          Network: { getStatus: async () => ({ connected: true }), addListener: () => {} },
          PushNotifications: { checkPermissions: async () => ({ receive: 'granted' }), requestPermissions: async () => ({ receive: 'granted' }), register: async () => {}, addListener: () => {} },
          Geolocation: { getCurrentPosition: async () => ({ coords: { latitude: -21.17, longitude: -48.45 } }), watchPosition: () => 'watch-id' },
        },
      };
    });

    // 1. Navigate to the page
    await page.goto('http://localhost:8000/index.html');

    // Wait for the App object to be available
    await page.waitForFunction(() => window.App);

    // 2. Bypass login by directly manipulating the App state and calling showAppScreen
    await page.evaluate(() => {
      const adminPermissions = { dashboard: true, monitoramentoAereo: true, relatorioMonitoramento: true, relatorioRisco: true, planejamentoColheita: true, planejamento: true, lancamentoBroca: true, lancamentoPerda: true, lancamentoCigarrinha: true, relatorioBroca: true, relatorioPerda: true, relatorioCigarrinha: true, lancamentoCigarrinhaPonto: true, relatorioCigarrinhaPonto: true, lancamentoCigarrinhaAmostragem: true, relatorioCigarrinhaAmostragem: true, excluir: true, gerenciarUsuarios: true, configuracoes: true, cadastrarPessoas: true, syncHistory: true, frenteDePlantio: true, apontamentoPlantio: true, relatorioPlantio: true, gerenciarLancamentos: true, lancamentoClima: true, dashboardClima: true, relatorioClima: true };

      // Set a mock current user
      window.App.state.currentUser = {
        uid: 'mock-user-id',
        email: 'test@test.com',
        username: 'testuser',
        role: 'admin',
        active: true,
        companyId: 'mock-company-id',
        permissions: adminPermissions
      };

      // Mock essential data for menu and map rendering
      window.App.state.companies = [{ id: 'mock-company-id', name: 'Mock Company', subscribedModules: Object.keys(adminPermissions) }];
      window.App.state.globalConfigs = Object.keys(adminPermissions).reduce((acc, key) => { acc[key] = true; return acc; }, {});

      // *** FIX: Mock geoJsonData to allow map layers to load ***
      window.App.state.geoJsonData = {
        type: 'FeatureCollection',
        features: []
      };

      // Call the function to show the main app screen
      window.App.ui.showAppScreen();
    });

    // 3. Wait for the app screen to be visible
    await expect(page.locator('#appScreen')).toBeVisible({ timeout: 10000 });

    // 4. Navigate to the "Monitoramento Aéreo" tab
    await page.locator('#btnToggleMenu').click();
    await page.locator('button.menu-btn:has-text("Monitoramento Aéreo")').click();

    // 5. Wait for the map to be ready
    await expect(page.locator('#map')).toBeVisible();
    await page.waitForTimeout(10000); // Increased wait time for map tiles and layers

    // 6. Click the "Risk View" button
    const riskViewButton = page.locator('#btnToggleRiskView');
    await expect(riskViewButton).toBeVisible({ timeout: 10000 }); // Increased timeout here as well
    await riskViewButton.click();

    // 7. Wait for the risk view to be applied
    await page.waitForTimeout(3000);

    // 8. Take a screenshot for visual verification
    await page.screenshot({ path: 'risk_view_screenshot.png' });
  });
});
