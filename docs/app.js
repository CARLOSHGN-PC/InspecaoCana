// FIREBASE: Importe os módulos necessários do Firebase SDK
import { initializeApp } from "https://www.gstatic.com/firebasejs/9.15.0/firebase-app.js";
import { getFirestore, collection, onSnapshot, doc, getDoc, addDoc, setDoc, updateDoc, deleteDoc, writeBatch, serverTimestamp, query, where, getDocs, enableIndexedDbPersistence, Timestamp, orderBy } from "https://www.gstatic.com/firebasejs/9.15.0/firebase-firestore.js";
import { getAuth, createUserWithEmailAndPassword, signInWithEmailAndPassword, signOut, onAuthStateChanged, updatePassword, sendPasswordResetEmail, EmailAuthProvider, reauthenticateWithCredential, setPersistence, browserSessionPersistence, browserLocalPersistence } from "https://www.gstatic.com/firebasejs/9.15.0/firebase-auth.js";
import { getStorage, ref, uploadBytes, getDownloadURL } from "https://www.gstatic.com/firebasejs/9.15.0/firebase-storage.js";
// Importa a biblioteca para facilitar o uso do IndexedDB (cache offline)
import { openDB } from 'https://unpkg.com/idb@7.1.1/build/index.js';

document.addEventListener('DOMContentLoaded', () => {

    // Lógica da Tela de Abertura
    const splashScreen = document.getElementById('splash-screen');
    if (splashScreen) {
        // Esconde a tela de abertura após a animação e um pequeno atraso
        setTimeout(() => {
            splashScreen.classList.add('hidden');
        }, 2500); // Ajuste o tempo conforme necessário
    }

    const firebaseConfig = {
        apiKey: "AIzaSyBFXgXKDIBo9JD9vuGik5VDYZFDb_tbCrY",
        authDomain: "agrovetor-v2.firebaseapp.com",
        projectId: "agrovetor-v2",
        storageBucket: "agrovetor-v2.firebasestorage.app",
        messagingSenderId: "782518751171",
        appId: "1:782518751171:web:d501ee31c1db33da4eb776",
        measurementId: "G-JN4MSW63JR"
    };

    const firebaseApp = initializeApp(firebaseConfig);
    const db = getFirestore(firebaseApp);
    const auth = getAuth(firebaseApp);
    const storage = getStorage(firebaseApp);
    
    const secondaryApp = initializeApp(firebaseConfig, "secondary");
    const secondaryAuth = getAuth(secondaryApp);

    // Adiciona as definições de projeção para o Proj4js
    if (window.proj4) {
        // Definição para SIRGAS 2000 geográfico (graus)
        proj4.defs("EPSG:4674", "+proj=longlat +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +no_defs");
        // Definição para SIRGAS 2000 / UTM zone 22S (metros) - a mais provável para o SHP
        proj4.defs("EPSG:31982", "+proj=utm +zone=22 +south +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs");
        // Definição padrão para WGS84 (usado pelo Mapbox)
        proj4.defs("WGS84", "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs");
    } else {
        console.error("Proj4js não foi carregado. A reprojeção de coordenadas não funcionará.");
    }


    enableIndexedDbPersistence(db)
        .catch((err) => {
            if (err.code == 'failed-precondition') {
                console.warn("A persistência offline falhou. Múltiplas abas abertas?");
            } else if (err.code == 'unimplemented') {
                console.warn("O navegador atual não suporta a persistência offline.");
            }
        });

    Chart.register(ChartDataLabels);
    Chart.defaults.font.family = "'Poppins', sans-serif";

    // Módulo para gerenciar o banco de dados local (IndexedDB)
    const OfflineDB = {
        dbPromise: null,
        async init() {
            if (this.dbPromise) return;
            // Version 6 for the new offline-credentials store
            this.dbPromise = openDB('agrovetor-offline-storage', 6, {
                upgrade(db, oldVersion) {
                    if (oldVersion < 1) {
                        db.createObjectStore('shapefile-cache');
                    }
                    if (oldVersion < 2) {
                        db.createObjectStore('offline-writes', { autoIncrement: true });
                    }
                    if (oldVersion < 3) {
                        db.createObjectStore('sync-history', { keyPath: 'timestamp' });
                    }
                    if (oldVersion < 4) {
                        db.createObjectStore('notifications', { autoIncrement: true });
                    }
                    if (oldVersion < 5) {
                        db.createObjectStore('gps-locations', { autoIncrement: true });
                    }
                    if (oldVersion < 6) {
                        db.createObjectStore('offline-credentials', { keyPath: 'email' });
                    }
                },
            });
        },
        async get(storeName, key) {
            return (await this.dbPromise).get(storeName, key);
        },
        async getAll(storeName) {
            return (await this.dbPromise).getAll(storeName);
        },
        async set(storeName, value, key) {
            return (await this.dbPromise).put(storeName, value, key);
        },
        async add(storeName, val) {
            return (await this.dbPromise).add(storeName, val);
        },
        async delete(storeName, key) {
            return (await this.dbPromise).delete(storeName, key);
        },
    };


    const App = {
        config: {
            appName: "Inspeção e Planejamento de Cana com IA",
            themeKey: 'canaAppTheme',
            inactivityTimeout: 15 * 60 * 1000,
            inactivityWarningTime: 1 * 60 * 1000,
            backendUrl: 'https://agrovetor-backend.onrender.com', // URL do seu backend
            menuConfig: [
                { label: 'Dashboard', icon: 'fas fa-tachometer-alt', target: 'dashboard', permission: 'dashboard' },
                { label: 'Dashboard Climatológico', icon: 'fas fa-cloud-sun-rain', target: 'dashboardClima', permission: 'dashboardClima' },
                { label: 'Monitoramento Aéreo', icon: 'fas fa-satellite-dish', target: 'monitoramentoAereo', permission: 'monitoramentoAereo' },
                { label: 'Plan. Inspeção', icon: 'fas fa-calendar-alt', target: 'planejamento', permission: 'planejamento' },
                {
                    label: 'Colheita', icon: 'fas fa-tractor',
                    submenu: [
                        { label: 'Planejamento de Colheita', icon: 'fas fa-stream', target: 'planejamentoColheita', permission: 'planejamentoColheita' },
                    ]
                },
                {
                    label: 'Lançamentos', icon: 'fas fa-pen-to-square',
                    submenu: [
                        { label: 'Lançamento Broca', icon: 'fas fa-bug', target: 'lancamentoBroca', permission: 'lancamentoBroca' },
                        { label: 'Lançamento Perda', icon: 'fas fa-dollar-sign', target: 'lancamentoPerda', permission: 'lancamentoPerda' },
                        { label: 'Monitoramento Cigarrinha', icon: 'fas fa-leaf', target: 'lancamentoCigarrinha', permission: 'lancamentoCigarrinha' },
                        { label: 'Monitoramento de Cigarrinha (Amostragem)', icon: 'fas fa-vial', target: 'lancamentoCigarrinhaAmostragem', permission: 'lancamentoCigarrinhaAmostragem' },
                        { label: 'Apontamento de Plantio', icon: 'fas fa-seedling', target: 'apontamentoPlantio', permission: 'apontamentoPlantio' },
                        { label: 'Apontamento Climatológico', icon: 'fas fa-cloud', target: 'lancamentoClima', permission: 'lancamentoClima' },
                    ]
                },
                {
                    label: 'Relatórios', icon: 'fas fa-chart-line',
                    submenu: [
                        { label: 'Relatório Broca', icon: 'fas fa-chart-bar', target: 'relatorioBroca', permission: 'relatorioBroca' },
                        { label: 'Relatório Perda', icon: 'fas fa-chart-pie', target: 'relatorioPerda', permission: 'relatorioPerda' },
                        { label: 'Relatório Cigarrinha', icon: 'fas fa-leaf', target: 'relatorioCigarrinha', permission: 'relatorioCigarrinha' },
                        { label: 'Rel. Cigarrinha (Amostragem)', icon: 'fas fa-file-invoice', target: 'relatorioCigarrinhaAmostragem', permission: 'relatorioCigarrinhaAmostragem' },
                        { label: 'Rel. Colheita Custom', icon: 'fas fa-file-invoice', target: 'relatorioColheitaCustom', permission: 'planejamentoColheita' },
                        { label: 'Rel. Monitoramento', icon: 'fas fa-map-marked-alt', target: 'relatorioMonitoramento', permission: 'relatorioMonitoramento' },
                        { label: 'Relatório de Risco', icon: 'fas fa-shield-alt', target: 'relatorioRisco', permission: 'relatorioRisco' },
                        { label: 'Relatórios de Plantio', icon: 'fas fa-chart-bar', target: 'relatorioPlantio', permission: 'relatorioPlantio' },
                        { label: 'Relatório Climatológico', icon: 'fas fa-file-pdf', target: 'relatorioClima', permission: 'relatorioClima' },
                    ]
                },
                {
                    label: 'Administrativo', icon: 'fas fa-cogs',
                    submenu: [
                        { label: 'Frente de Plantio', icon: 'fas fa-tractor', target: 'frenteDePlantio', permission: 'frenteDePlantio' },
                        { label: 'Cadastros', icon: 'fas fa-book', target: 'cadastros', permission: 'configuracoes' },
                        { label: 'Cadastrar Pessoas', icon: 'fas fa-id-card', target: 'cadastrarPessoas', permission: 'cadastrarPessoas' },
                        { label: 'Gerir Utilizadores', icon: 'fas fa-users-cog', target: 'gerenciarUsuarios', permission: 'gerenciarUsuarios' },
                        { label: 'Configurações da Empresa', icon: 'fas fa-building', target: 'configuracoesEmpresa', permission: 'configuracoes' },
                        { label: 'Histórico de Sincronização', icon: 'fas fa-history', target: 'syncHistory', permission: 'syncHistory' },
                        { label: 'Gerenciar Lançamentos', icon: 'fas fa-edit', target: 'gerenciarLancamentos', permission: 'gerenciarLancamentos' },
                    ]
                },
                {
                    label: 'Super Admin', icon: 'fas fa-user-shield',
                    submenu: [
                        { label: 'Gerir Empresas', icon: 'fas fa-building', target: 'gerenciarEmpresas', permission: 'superAdmin' },
                    ]
                }
            ],
            roles: {
                admin: { dashboard: true, monitoramentoAereo: true, relatorioMonitoramento: true, relatorioRisco: true, planejamentoColheita: true, planejamento: true, lancamentoBroca: true, lancamentoPerda: true, lancamentoCigarrinha: true, relatorioBroca: true, relatorioPerda: true, relatorioCigarrinha: true, lancamentoCigarrinhaPonto: true, relatorioCigarrinhaPonto: true, lancamentoCigarrinhaAmostragem: true, relatorioCigarrinhaAmostragem: true, excluir: true, gerenciarUsuarios: true, configuracoes: true, cadastrarPessoas: true, syncHistory: true, frenteDePlantio: true, apontamentoPlantio: true, relatorioPlantio: true, gerenciarLancamentos: true, lancamentoClima: true, dashboardClima: true, relatorioClima: true },
                supervisor: { dashboard: true, monitoramentoAereo: true, relatorioMonitoramento: true, relatorioRisco: true, planejamentoColheita: true, planejamento: true, lancamentoCigarrinha: true, relatorioBroca: true, relatorioPerda: true, relatorioCigarrinha: true, lancamentoCigarrinhaPonto: true, relatorioCigarrinhaPonto: true, lancamentoCigarrinhaAmostragem: true, relatorioCigarrinhaAmostragem: true, configuracoes: true, cadastrarPessoas: true, gerenciarUsuarios: true, frenteDePlantio: true, apontamentoPlantio: true, relatorioPlantio: true, gerenciarLancamentos: true, lancamentoClima: true, dashboardClima: true, relatorioClima: true },
                tecnico: { dashboard: true, monitoramentoAereo: true, relatorioMonitoramento: true, relatorioRisco: true, lancamentoBroca: true, lancamentoPerda: true, lancamentoCigarrinha: true, relatorioBroca: true, relatorioPerda: true, relatorioCigarrinha: true, lancamentoCigarrinhaPonto: true, relatorioCigarrinhaPonto: true, lancamentoCigarrinhaAmostragem: true, relatorioCigarrinhaAmostragem: true, apontamentoPlantio: true, relatorioPlantio: true, lancamentoClima: true, dashboardClima: true, relatorioClima: true },
                colaborador: { dashboard: true, monitoramentoAereo: true, lancamentoBroca: true, lancamentoPerda: true, lancamentoClima: true, dashboardClima: true, relatorioClima: true },
                user: { dashboard: true }
            }
        },

        state: {
            isImpersonating: false,
            originalUser: null,
            isSyncing: false,
            isCheckingConnection: false,
            connectionCheckInterval: null,
            currentUser: null,
            users: [],
            companies: [],
            globalConfigs: {}, // NOVO: Para armazenar configurações globais como feature flags
            companyConfig: {},
            registros: [],
            perdas: [],
            cigarrinha: [],
            planos: [],
            fazendas: [],
            personnel: [],
            frentesDePlantio: [],
            apontamentosPlantio: [],
            companyLogo: null,
            activeSubmenu: null,
            charts: {},
            harvestPlans: [],
            activeHarvestPlan: null,
            inactivityTimer: null,
            inactivityWarningTimer: null,
            unsubscribeListeners: [],
            deferredInstallPrompt: null,
            adminAction: null, // Stores a function to be executed after admin password confirmation
            expandedChart: null,
            mapboxMap: null,
            mapboxUserMarker: null,
            mapboxTrapMarkers: {},
            armadilhas: [],
            geoJsonData: null,
            selectedMapFeature: null, // NOVO: Armazena a feature do talhão selecionado no mapa
            trapNotifications: [],
            unreadNotificationCount: 0,
            notifiedTrapIds: new Set(JSON.parse(sessionStorage.getItem('notifiedTrapIds')) || []),
            trapPlacementMode: null,
            trapPlacementData: null,
            locationWatchId: null,
            locationUpdateIntervalId: null,
            lastKnownPosition: null,
            riskViewActive: false,
            isTracking: false,
            plantio: [], // Placeholder for Plantio data
            cigarrinha: [], // Placeholder for Cigarrinha data
            clima: [],
            apontamentoPlantioFormIsDirty: false,
            syncInterval: null,
        },
        
        elements: {
            loadingOverlay: document.getElementById('loading-overlay'),
            loadingProgressText: document.getElementById('loading-progress-text'),
            loginScreen: document.getElementById('loginScreen'),
            appScreen: document.getElementById('appScreen'),
            loginUser: document.getElementById('loginUser'),
            loginPass: document.getElementById('loginPass'),
            btnLogin: document.getElementById('btnLogin'),
            loginMessage: document.getElementById('loginMessage'),
            loginForm: document.getElementById('loginForm'),
            offlineUserSelection: document.getElementById('offlineUserSelection'),
            offlineUserList: document.getElementById('offlineUserList'),
            headerTitle: document.querySelector('header h1'),
            headerLogo: document.getElementById('headerLogo'),
            currentDateTime: document.getElementById('currentDateTime'),
            logoutBtn: document.getElementById('logoutBtn'),
            btnToggleMenu: document.getElementById('btnToggleMenu'),
            menu: document.getElementById('menu'),
            content: document.getElementById('content'),
            alertContainer: document.getElementById('alertContainer'),
            notificationContainer: document.getElementById('notification-container'),
            notificationBell: {
                container: document.getElementById('notification-bell-container'),
                toggle: document.getElementById('notification-bell-toggle'),
                count: document.getElementById('notification-count'),
                dropdown: document.getElementById('notification-dropdown'),
                list: document.getElementById('notification-list'), // NOVO
                clearBtn: document.getElementById('clear-notifications-btn'), // NOVO
                noNotifications: document.getElementById('no-notifications'), // NOVO
            },
            userMenu: {
                container: document.getElementById('user-menu-container'),
                toggle: document.getElementById('user-menu-toggle'),
                dropdown: document.getElementById('user-menu-dropdown'),
                username: document.getElementById('userMenuUsername'),
                changePasswordBtn: document.getElementById('changePasswordBtn'),
                manualSyncBtn: document.getElementById('manualSyncBtn'),
                themeButtons: document.querySelectorAll('.theme-button')
            },
            confirmationModal: {
                overlay: document.getElementById('confirmationModal'),
                title: document.getElementById('confirmationModalTitle'),
                message: document.getElementById('confirmationModalMessage'),
                confirmBtn: document.getElementById('confirmationModalConfirmBtn'),
                cancelBtn: document.getElementById('confirmationModalCancelBtn'),
                closeBtn: document.getElementById('confirmationModalCloseBtn'),
                inputContainer: document.getElementById('confirmationModalInputContainer'),
                input: document.getElementById('confirmationModalInput'),
            },
            changePasswordModal: {
                overlay: document.getElementById('changePasswordModal'),
                closeBtn: document.getElementById('changePasswordModalCloseBtn'),
                cancelBtn: document.getElementById('changePasswordModalCancelBtn'),
                saveBtn: document.getElementById('changePasswordModalSaveBtn'),
                currentPassword: document.getElementById('currentPassword'),
                newPassword: document.getElementById('newPassword'),
                confirmNewPassword: document.getElementById('confirmNewPassword'),
            },
            adminPasswordConfirmModal: {
                overlay: document.getElementById('adminPasswordConfirmModal'),
                closeBtn: document.getElementById('adminPasswordConfirmModalCloseBtn'),
                cancelBtn: document.getElementById('adminPasswordConfirmModalCancelBtn'),
                confirmBtn: document.getElementById('adminPasswordConfirmModalConfirmBtn'),
                passwordInput: document.getElementById('adminConfirmPassword')
            },
            chartModal: {
                overlay: document.getElementById('chartModal'),
                title: document.getElementById('chartModalTitle'),
                closeBtn: document.getElementById('chartModalCloseBtn'),
                canvas: document.getElementById('expandedChartCanvas'),
            },
            editFarmModal: {
                overlay: document.getElementById('editFarmModal'),
                closeBtn: document.getElementById('editFarmModalCloseBtn'),
                cancelBtn: document.getElementById('editFarmModalCancelBtn'),
                saveBtn: document.getElementById('editFarmModalSaveBtn'),
                nameInput: document.getElementById('editFarmNameInput'),
                editingFarmId: document.getElementById('editingFarmId'),
                typeCheckboxes: document.querySelectorAll('#editFarmTypeCheckboxes input[type="checkbox"]'),
            },
             historyFilterModal: {
                overlay: document.getElementById('historyFilterModal'),
                closeBtn: document.getElementById('historyFilterModalCloseBtn'),
                cancelBtn: document.getElementById('historyFilterModalCancelBtn'),
                viewBtn: document.getElementById('btnViewHistoryModal'),
                clearBtn: document.getElementById('btnClearHistoryModal'),
                userSelect: document.getElementById('historyUserSelectModal'),
                startDate: document.getElementById('historyStartDateModal'),
                endDate: document.getElementById('historyEndDateModal'),
            },
            syncHistoryDetailModal: {
                overlay: document.getElementById('syncHistoryDetailModal'),
                title: document.getElementById('syncHistoryDetailModalTitle'),
                body: document.getElementById('syncHistoryDetailModalBody'),
                closeBtn: document.getElementById('syncHistoryDetailModalCloseBtn'),
                cancelBtn: document.getElementById('syncHistoryDetailModalCancelBtn'),
            },
            configHistoryModal: {
                overlay: document.getElementById('configHistoryModal'),
                title: document.getElementById('configHistoryModalTitle'),
                body: document.getElementById('configHistoryModalBody'),
                closeBtn: document.getElementById('configHistoryModalCloseBtn'),
                cancelBtn: document.getElementById('configHistoryModalCancelBtn'),
            },
            companyConfig: {
                logoUploadArea: document.getElementById('logoUploadArea'),
                logoInput: document.getElementById('logoInput'),
                logoPreview: document.getElementById('logoPreview'),
                removeLogoBtn: document.getElementById('removeLogoBtn'),
                progressUploadArea: document.getElementById('harvestReportProgressUploadArea'),
                progressInput: document.getElementById('harvestReportProgressInput'),
                btnDownloadProgressTemplate: document.getElementById('btnDownloadProgressTemplate'),
                closedUploadArea: document.getElementById('harvestReportClosedUploadArea'),
                closedInput: document.getElementById('harvestReportClosedInput'),
                btnDownloadClosedTemplate: document.getElementById('btnDownloadClosedTemplate'),
                shapefileUploadArea: document.getElementById('shapefileUploadArea'),
                shapefileInput: document.getElementById('shapefileInput'),
                historicalReportUploadArea: document.getElementById('historicalReportUploadArea'),
                historicalReportInput: document.getElementById('historicalReportInput'),
                btnDownloadHistoricalTemplate: document.getElementById('btnDownloadHistoricalTemplate'),
                btnDeleteHistoricalData: document.getElementById('btnDeleteHistoricalData'),
            },
            dashboard: {
                selector: document.getElementById('dashboard-selector'),
                brocaView: document.getElementById('dashboard-broca'),
                perdaView: document.getElementById('dashboard-perda'),
                aereaView: document.getElementById('dashboard-aerea'),
                plantioView: document.getElementById('dashboard-plantio'),
                cigarrinhaView: document.getElementById('dashboard-cigarrinha'),
                climaView: document.getElementById('dashboard-clima'),
                cardBroca: document.getElementById('card-broca'),
                cardPerda: document.getElementById('card-perda'),
                cardAerea: document.getElementById('card-aerea'),
                cardPlantio: document.getElementById('card-plantio'),
                cardCigarrinha: document.getElementById('card-cigarrinha'),
                cardClima: document.getElementById('card-clima'),
                btnBackToSelectorBroca: document.getElementById('btn-back-to-selector-broca'),
                btnBackToSelectorPerda: document.getElementById('btn-back-to-selector-perda'),
                btnBackToSelectorAerea: document.getElementById('btn-back-to-selector-aerea'),
                btnBackToSelectorPlantio: document.getElementById('btn-back-to-selector-plantio'),
                btnBackToSelectorCigarrinha: document.getElementById('btn-back-to-selector-cigarrinha'),
                btnBackToSelectorClima: document.getElementById('btn-back-to-selector-clima'),
                brocaDashboardInicio: document.getElementById('brocaDashboardInicio'),
                brocaDashboardFim: document.getElementById('brocaDashboardFim'),
                btnFiltrarBrocaDashboard: document.getElementById('btnFiltrarBrocaDashboard'),
                perdaDashboardInicio: document.getElementById('perdaDashboardInicio'),
                perdaDashboardFim: document.getElementById('perdaDashboardFim'),
                btnFiltrarPerdaDashboard: document.getElementById('btnFiltrarPerdaDashboard'),
            },
            users: {
                username: document.getElementById('newUserUsername'),
                password: document.getElementById('newUserPassword'),
                role: document.getElementById('newUserRole'),
                permissionsContainer: document.querySelector('#gerenciarUsuarios .permission-grid'),
                permissionCheckboxes: document.querySelectorAll('#gerenciarUsuarios .permission-grid input[type="checkbox"]'),
                btnCreate: document.getElementById('btnCreateUser'),
                list: document.getElementById('usersList'),
                superAdminUserCreation: document.getElementById('superAdminUserCreation'),
                adminTargetCompanyUsers: document.getElementById('adminTargetCompanyUsers'),
            },
            userEditModal: {
                overlay: document.getElementById('userEditModal'),
                title: document.getElementById('userEditModalTitle'),
                closeBtn: document.getElementById('userEditModalCloseBtn'),
                editingUserId: document.getElementById('editingUserId'),
                username: document.getElementById('editUserUsername'),
                role: document.getElementById('editUserRole'),
                permissionGrid: document.getElementById('editUserPermissionGrid'),
                btnSaveChanges: document.getElementById('btnSaveUserChanges'),
                btnResetPassword: document.getElementById('btnResetPassword'),
                btnDeleteUser: document.getElementById('btnDeleteUser'),
            },
            companyManagement: {
                companyName: document.getElementById('newCompanyName'),
                adminEmail: document.getElementById('newCompanyAdminEmail'),
                adminPassword: document.getElementById('newCompanyAdminPassword'),
                btnCreate: document.getElementById('btnCreateCompany'),
                list: document.getElementById('companiesList'),
            },
            editCompanyModal: {
                overlay: document.getElementById('editCompanyModal'),
                title: document.getElementById('editCompanyModalTitle'),
                closeBtn: document.getElementById('editCompanyModalCloseBtn'),
                cancelBtn: document.getElementById('editCompanyModalCancelBtn'),
                saveBtn: document.getElementById('editCompanyModalSaveBtn'),
                editingCompanyId: document.getElementById('editingCompanyId'),
                companyNameDisplay: document.getElementById('editCompanyNameDisplay'),
                modulesGrid: document.getElementById('editCompanyModulesGrid'),
            },
            personnel: {
                id: document.getElementById('personnelId'),
                matricula: document.getElementById('personnelMatricula'),
                name: document.getElementById('personnelName'),
                btnSave: document.getElementById('btnSavePersonnel'),
                list: document.getElementById('personnelList'),
                csvUploadArea: document.getElementById('personnelCsvUploadArea'),
                csvFileInput: document.getElementById('personnelCsvInput'),
                btnDownloadCsvTemplate: document.getElementById('btnDownloadPersonnelCsvTemplate'),
            },
            frenteDePlantio: {
                id: document.getElementById('frenteDePlantioId'),
                name: document.getElementById('frenteDePlantioName'),
                provider: document.getElementById('frenteDePlantioProvider'),
                obs: document.getElementById('frenteDePlantioObs'),
                btnSave: document.getElementById('btnSaveFrenteDePlantio'),
                list: document.getElementById('frenteDePlantioList'),
            },
            apontamentoPlantio: {
                form: document.getElementById('formApontamentoPlantio'),
                entryId: document.getElementById('plantioEntryId'),
                frente: document.getElementById('plantioFrente'),
                provider: document.getElementById('plantioProvider'),
                culture: document.getElementById('plantioCulture'),
                leaderId: document.getElementById('plantioLeaderId'),
                leaderName: document.getElementById('plantioLeaderName'),
                farmName: document.getElementById('plantioFarmName'),
                date: document.getElementById('plantioDate'),
                addRecordBtn: document.getElementById('addPlantioRecord'),
                recordsContainer: document.getElementById('plantioRecordsContainer'),
                totalArea: document.getElementById('totalPlantedArea'),
                btnSave: document.getElementById('btnSaveApontamentoPlantio'),
                chuva: document.getElementById('plantioChuva'),
                obs: document.getElementById('plantioObs'),
                info: document.getElementById('plantioInfo'),
            },
            cadastros: {
                farmCode: document.getElementById('farmCode'),
                farmName: document.getElementById('farmName'),
                farmTypeCheckboxes: document.querySelectorAll('#farmTypeCheckboxes input[type="checkbox"]'),
                btnSaveFarm: document.getElementById('btnSaveFarm'),
                btnDeleteAllFarms: document.getElementById('btnDeleteAllFarms'),
                farmSelect: document.getElementById('farmSelect'),
                talhaoManagementContainer: document.getElementById('talhaoManagementContainer'),
                selectedFarmName: document.getElementById('selectedFarmName'),
                selectedFarmTypes: document.getElementById('selectedFarmTypes'),
                talhaoList: document.getElementById('talhaoList'),
                talhaoId: document.getElementById('talhaoId'),
                talhaoName: document.getElementById('talhaoName'),
                talhaoArea: document.getElementById('talhaoArea'),
                talhaoTCH: document.getElementById('talhaoTCH'),
                talhaoProducao: document.getElementById('talhaoProducao'),
                talhaoCorte: document.getElementById('talhaoCorte'),
                talhaoVariedade: document.getElementById('talhaoVariedade'),
                talhaoDistancia: document.getElementById('talhaoDistancia'),
                talhaoUltimaColheita: document.getElementById('talhaoUltimaColheita'),
                btnSaveTalhao: document.getElementById('btnSaveTalhao'),
                csvUploadArea: document.getElementById('csvUploadArea'),
                csvFileInput: document.getElementById('csvFileInput'),
                btnDownloadCsvTemplate: document.getElementById('btnDownloadCsvTemplate'),
                superAdminFarmCreation: document.getElementById('superAdminFarmCreation'),
                adminTargetCompanyFarms: document.getElementById('adminTargetCompanyFarms'),
                importProgress: {
                    container: document.getElementById('farm-import-progress'),
                    text: document.querySelector('#farm-import-progress .download-progress-text'),
                    bar: document.querySelector('#farm-import-progress .download-progress-bar'),
                }
            },
            planejamento: {
                tipo: document.getElementById('planoTipo'),
                fazenda: document.getElementById('planoFazenda'),
                talhao: document.getElementById('planoTalhao'),
                data: document.getElementById('planoData'),
                responsavel: document.getElementById('planoResponsavel'),
                meta: document.getElementById('planoMeta'),
                obs: document.getElementById('planoObs'),
                btnAgendar: document.getElementById('btnAgendarInspecao'),
                btnSugerir: document.getElementById('btnSugerirPlano'),
                lista: document.getElementById('listaPlanejamento')
            },
            harvest: {
                plansListContainer: document.getElementById('harvest-plans-list-container'),
                plansList: document.getElementById('harvest-plans-list'),
                planEditor: document.getElementById('harvest-plan-editor'),
                btnAddNew: document.getElementById('btnAddNewHarvestPlan'),
                maturador: document.getElementById('harvestMaturador'),
                maturadorDate: document.getElementById('harvestMaturadorDate'),
                btnSavePlan: document.getElementById('btnSaveHarvestPlan'),
                btnCancelPlan: document.getElementById('btnCancelHarvestPlan'),
                frontName: document.getElementById('harvestFrontName'),
                startDate: document.getElementById('harvestStartDate'),
                dailyRate: document.getElementById('harvestDailyRate'),
                fazenda: document.getElementById('harvestFazenda'),
                atr: document.getElementById('harvestAtr'),
                talhaoSelectionList: document.getElementById('harvestTalhaoSelectionList'),
                selectAllTalhoes: document.getElementById('selectAllTalhoes'),
                btnAddOrUpdate: document.getElementById('btnAddOrUpdateHarvestSequence'),
                btnCancelEdit: document.getElementById('btnCancelEditSequence'),
                addOrEditTitle: document.getElementById('addOrEditSequenceTitle'),
                editingGroupId: document.getElementById('editingGroupId'),
                btnOptimize: document.getElementById('btnOptimizeHarvest'),
                tableBody: document.querySelector('#harvestPlanTable tbody'),
                summary: document.getElementById('harvestSummary'),
                superAdminHarvestCreation: document.getElementById('superAdminHarvestCreation'),
                adminTargetCompanyHarvest: document.getElementById('adminTargetCompanyHarvest'),
            },
            broca: {
                form: document.getElementById('lancamentoBroca'),
                codigo: document.getElementById('codigo'),
                data: document.getElementById('data'),
                talhao: document.getElementById('talhao'),
                varietyDisplay: document.getElementById('varietyDisplay'),
                entrenos: document.getElementById('entrenos'),
                base: document.getElementById('brocaBase'),
                meio: document.getElementById('brocaMeio'),
                topo: document.getElementById('brocaTopo'),
                brocado: document.getElementById('brocado'),
                resultado: document.getElementById('resultado'),
                btnSalvar: document.getElementById('btnSalvarBrocamento'),
                filtroFazenda: document.getElementById('fazendaFiltroBrocamento'),
                tipoRelatorio: document.getElementById('tipoRelatorioBroca'),
                filtroInicio: document.getElementById('inicioBrocamento'),
                filtroFim: document.getElementById('fimBrocamento'),
                farmTypeFilter: document.querySelectorAll('#brocaReportFarmTypeFilter input[type="checkbox"]'),
                btnPDF: document.getElementById('btnPDFBrocamento'),
                btnExcel: document.getElementById('btnExcelBrocamento'),
            },
            perda: {
                form: document.getElementById('lancamentoPerda'),
                data: document.getElementById('dataPerda'),
                codigo: document.getElementById('codigoPerda'),
                talhao: document.getElementById('talhaoPerda'),
                varietyDisplay: document.getElementById('varietyDisplayPerda'),
                frente: document.getElementById('frenteServico'),
                turno: document.getElementById('turno'),
                frota: document.getElementById('frotaEquipamento'),
                matricula: document.getElementById('matriculaOperador'),
                operadorNome: document.getElementById('operadorNome'),
                canaInteira: document.getElementById('canaInteira'),
                tolete: document.getElementById('tolete'),
                toco: document.getElementById('toco'),
                ponta: document.getElementById('ponta'),
                estilhaco: document.getElementById('estilhaco'),
                pedaco: document.getElementById('pedaco'),
                resultado: document.getElementById('resultadoPerda'),
                btnSalvar: document.getElementById('btnSalvarPerda'),
                filtroFazenda: document.getElementById('fazendaFiltroPerda'),
                filtroTalhao: document.getElementById('talhaoFiltroPerda'),
                filtroOperador: document.getElementById('operadorFiltroPerda'),
                filtroFrente: document.getElementById('frenteFiltroPerda'),
                filtroInicio: document.getElementById('inicioPerda'),
                filtroFim: document.getElementById('fimPerda'),
                farmTypeFilter: document.querySelectorAll('#perdaReportFarmTypeFilter input[type="checkbox"]'),
                tipoRelatorio: document.getElementById('tipoRelatorioPerda'),
                btnPDF: document.getElementById('btnPDFPerda'),
                btnExcel: document.getElementById('btnExcelPerda'),
            },
            cigarrinha: {
                form: document.getElementById('lancamentoCigarrinha'),
                data: document.getElementById('dataCigarrinha'),
                codigo: document.getElementById('codigoCigarrinha'),
                talhao: document.getElementById('talhaoCigarrinha'),
                varietyDisplay: document.getElementById('varietyDisplayCigarrinha'),
                fase1: document.getElementById('fase1Cigarrinha'),
                fase2: document.getElementById('fase2Cigarrinha'),
                fase3: document.getElementById('fase3Cigarrinha'),
                fase4: document.getElementById('fase4Cigarrinha'),
                fase5: document.getElementById('fase5Cigarrinha'),
                adulto: document.getElementById('adultoPresenteCigarrinha'),
                resultado: document.getElementById('resultadoCigarrinha'),
                btnSalvar: document.getElementById('btnSalvarCigarrinha'),
                filtroFazenda: document.getElementById('fazendaFiltroCigarrinha'),
                filtroInicio: document.getElementById('inicioCigarrinha'),
                filtroFim: document.getElementById('fimCigarrinha'),
                btnPDF: document.getElementById('btnPDFCigarrinha'),
                btnExcel: document.getElementById('btnExcelCigarrinha'),
            },
            cigarrinhaAmostragem: {
                form: document.getElementById('formCigarrinhaAmostragem'),
                data: document.getElementById('dataCigarrinhaAmostragem'),
                codigo: document.getElementById('codigoCigarrinhaAmostragem'),
                talhao: document.getElementById('talhaoCigarrinhaAmostragem'),
                varietyDisplay: document.getElementById('varietyDisplayCigarrinhaAmostragem'),
                addAmostraBtn: document.getElementById('addAmostraCigarrinhaAmostragem'),
                amostrasContainer: document.getElementById('amostrasCigarrinhaAmostragemContainer'),
                adulto: document.getElementById('adultoPresenteCigarrinhaAmostragem'),
                resultado: document.getElementById('resultadoCigarrinhaAmostragem'),
                btnSalvar: document.getElementById('btnSalvarCigarrinhaAmostragem'),
                filtroFazenda: document.getElementById('fazendaFiltroCigarrinhaAmostragem'),
                filtroInicio: document.getElementById('inicioCigarrinhaAmostragem'),
                filtroFim: document.getElementById('fimCigarrinhaAmostragem'),
                btnPDF: document.getElementById('btnPDFCigarrinhaAmostragem'),
                btnExcel: document.getElementById('btnExcelCigarrinhaAmostragem'),
            },
            gerenciamento: {
                lista: document.getElementById('listaGerenciamento'),
                dataType: document.getElementById('manageDataType'),
                startDate: document.getElementById('manageStartDate'),
                endDate: document.getElementById('manageEndDate'),
                applyBtn: document.getElementById('btnApplyManageFilters')
            },
            relatorioColheita: {
                select: document.getElementById('planoRelatorioSelect'),
                optionsContainer: document.getElementById('reportOptionsContainer'),
                colunasDetalhadoContainer: document.getElementById('colunas-detalhado-container'),
                tipoRelatorioSelect: document.getElementById('tipoRelatorioColheita'),
                btnPDF: document.getElementById('btnGerarRelatorioCustomPDF'),
                btnExcel: document.getElementById('btnGerarRelatorioCustomExcel'),
            },
            monitoramentoAereo: {
                container: document.getElementById('monitoramentoAereo-container'),
                mapContainer: document.getElementById('map'),
                btnAddTrap: document.getElementById('btnAddTrap'),
                btnCenterMap: document.getElementById('btnCenterMap'),
                btnHistory: document.getElementById('btnHistory'),
                btnToggleRiskView: document.getElementById('btnToggleRiskView'),
                infoBox: document.getElementById('talhao-info-box'),
                infoBoxContent: document.getElementById('talhao-info-box-content'),
                infoBoxCloseBtn: document.getElementById('close-info-box'),
                trapInfoBox: document.getElementById('trap-info-box'),
                trapInfoBoxContent: document.getElementById('trap-info-box-content'),
                trapInfoBoxCloseBtn: document.getElementById('close-trap-info-box'),
                    mapFarmSearchInput: document.getElementById('map-farm-search-input'),
                    mapFarmSearchBtn: document.getElementById('map-farm-search-btn'),
            },
            relatorioPlantio: {
                frente: document.getElementById('plantioRelatorioFrente'),
                cultura: document.getElementById('plantioRelatorioCultura'),
                inicio: document.getElementById('plantioRelatorioInicio'),
                fim: document.getElementById('plantioRelatorioFim'),
                tipo: document.getElementById('tipoRelatorioPlantio'),
                btnPDF: document.getElementById('btnPDFPlantio'),
                btnExcel: document.getElementById('btnExcelPlantio'),
            },
                lancamentoClima: {
                    form: document.getElementById('formLancamentoClima'),
                    entryId: document.getElementById('climaEntryId'),
                    data: document.getElementById('climaData'),
                    fazenda: document.getElementById('climaFazenda'),
                    talhao: document.getElementById('climaTalhao'),
                    tempMax: document.getElementById('climaTempMax'),
                    tempMin: document.getElementById('climaTempMin'),
                    umidade: document.getElementById('climaUmidade'),
                    pluviosidade: document.getElementById('climaPluviosidade'),
                    vento: document.getElementById('climaVento'),
                    obs: document.getElementById('climaObs'),
                    btnSave: document.getElementById('btnSaveLancamentoClima'),
                },
                relatorioClima: {
                    fazenda: document.getElementById('climaRelatorioFazenda'),
                    inicio: document.getElementById('climaRelatorioInicio'),
                    fim: document.getElementById('climaRelatorioFim'),
                    btnPDF: document.getElementById('btnPDFClima'),
                    btnExcel: document.getElementById('btnExcelClima'),
                },
            relatorioMonitoramento: {
                tipoRelatorio: document.getElementById('monitoramentoTipoRelatorio'),
                fazendaFiltro: document.getElementById('monitoramentoFazendaFiltro'),
                inicio: document.getElementById('monitoramentoInicio'),
                fim: document.getElementById('monitoramentoFim'),
                btnPDF: document.getElementById('btnPDFMonitoramento'),
                btnExcel: document.getElementById('btnExcelMonitoramento'),
            },
            relatorioRisco: {
                inicio: document.getElementById('riscoRelatorioInicio'),
                fim: document.getElementById('riscoRelatorioFim'),
                btnPDF: document.getElementById('btnPDFRisco'),
                btnExcel: document.getElementById('btnExcelRisco'),
            },
            trapPlacementModal: {
                overlay: document.getElementById('trapPlacementModal'),
                body: document.getElementById('trapPlacementModalBody'),
                closeBtn: document.getElementById('trapPlacementModalCloseBtn'),
                cancelBtn: document.getElementById('trapPlacementModalCancelBtn'),
                manualBtn: document.getElementById('trapPlacementModalManualBtn'),
                confirmBtn: document.getElementById('trapPlacementModalConfirmBtn'),
            },
            installAppBtn: document.getElementById('installAppBtn'),
        },

        isFeatureGloballyActive(featureKey) {
            // A funcionalidade está ativa se a flag correspondente for explicitamente `true`.
            // Se a flag não existir ou for `false`, a funcionalidade está desativada.
            return App.state.globalConfigs[featureKey] === true;
        },

        debounce(func, delay = 1000) {
            let timeout;
            return (...args) => {
                clearTimeout(timeout);
                timeout = setTimeout(() => {
                    func.apply(this, args);
                }, delay);
            };
        },

        init() {
            OfflineDB.init();
            this.native.init();
            this.ui.applyTheme(localStorage.getItem(this.config.themeKey) || 'theme-green');
            this.ui.setupEventListeners();
            this.auth.checkSession();
            this.pwa.registerServiceWorker();
        },

        native: {
            init() {
                if (window.Capacitor && Capacitor.isNativePlatform()) {
                    this.configureStatusBar();
                    this.registerPushNotifications();
                    this.listenForNetworkChanges(); // Adiciona o listener de rede
                }
            },

            // --- Funcionalidade 4: Monitoramento de Rede ---
            async listenForNetworkChanges() {
                try {
                    const { Network } = Capacitor.Plugins;

                    // Exibe o status inicial
                    const status = await Network.getStatus();
                    console.log(`Status inicial da rede: ${status.connected ? 'Online' : 'Offline'}`);

                    // Adiciona um 'ouvinte' para quando o status da rede mudar
                    Network.addListener('networkStatusChange', (status) => {
                        console.log(`Status da rede alterado para: ${status.connected ? 'Online' : 'Offline'}`);
                        if (status.connected) {
                            // Se conectar, dispara um evento 'online' personalizado,
                            // que a lógica existente do App já sabe como manipular.
                            window.dispatchEvent(new Event('online'));
                        }
                    });
                } catch (e) {
                    console.error("Erro ao configurar o monitoramento de rede do Capacitor.", e);
                }
            },

            // --- Funcionalidade 1: Correção da Barra de Status ---
            configureStatusBar() {
                try {
                    // Importa o plugin StatusBar. A variável 'Capacitor' é injetada pelo Capacitor.
                    const { StatusBar } = Capacitor.Plugins;

                    // Esta é a configuração chave.
                    // `setOverlaysWebView({ overlay: false })` instrui o Capacitor a não deixar
                    // a WebView (o conteúdo do seu app) sobrepor a barra de status.
                    // Em vez disso, a barra de status vai empurrar o conteúdo para baixo.
                    StatusBar.setOverlaysWebView({ overlay: false });

                    console.log("Status bar configurada para não sobrepor a webview.");

                } catch (e) {
                    console.error("Erro ao configurar a StatusBar do Capacitor.", e);
                }
            },

            // --- Funcionalidade 2: Geolocalização ---
            async getCurrentLocation() {
                try {
                    const { Geolocation } = Capacitor.Plugins;
                    const coordinates = await Geolocation.getCurrentPosition();
                    console.log('Localização Atual:', coordinates);
                    // Exemplo de como usar:
                    // App.ui.showAlert(`Lat: ${coordinates.coords.latitude}, Lng: ${coordinates.coords.longitude}`);
                    return coordinates;
                } catch (e) {
                    console.error("Erro ao obter localização", e);
                    App.ui.showAlert("Não foi possível obter a sua localização. Verifique as permissões do aplicativo.", "error");
                    return null;
                }
            },

            async watchLocation(callback) {
                try {
                    const { Geolocation } = Capacitor.Plugins;
                    // O watchPosition retorna um ID que pode ser usado para parar de observar
                    const watchId = await Geolocation.watchPosition({}, (position, err) => {
                        if (err) {
                            console.error("Erro ao observar a localização", err);
                            return;
                        }
                        console.log('Nova localização recebida:', position);
                        if (callback && typeof callback === 'function') {
                            callback(position);
                        }
                    });

                    // Para parar de observar a localização, você chamaria:
                    // const { Geolocation } = Capacitor.Plugins;
                    // Geolocation.clearWatch({ id: watchId });

                    return watchId;
                } catch (e) {
                    console.error("Erro ao iniciar o watchPosition", e);
                    App.ui.showAlert("Não foi possível iniciar o monitoramento de localização.", "error");
                    return null;
                }
            },

            // --- Funcionalidade 3: Notificações Push ---
            async registerPushNotifications() {
                const { PushNotifications } = Capacitor.Plugins;

                // 1. Verificar se a permissão já foi concedida
                let permStatus = await PushNotifications.checkPermissions();

                if (permStatus.receive === 'prompt') {
                    // 2. Se for a primeira vez, pedir permissão
                    permStatus = await PushNotifications.requestPermissions();
                }

                if (permStatus.receive !== 'granted') {
                    // 3. Se a permissão for negada, informar o usuário
                    App.ui.showAlert('A permissão para notificações não foi concedida.', 'warning');
                    return;
                }

                // 4. Se a permissão for concedida, registrar o dispositivo no serviço de push (FCM)
                await PushNotifications.register();

                // 5. Adicionar 'ouvintes' (listeners) para os eventos de notificação
                this.addPushNotificationListeners();
            },

            async addPushNotificationListeners() {
                const { PushNotifications } = Capacitor.Plugins;

                // Disparado ao receber o token de registro (FCM Token)
                PushNotifications.addListener('registration', async (token) => {
                    console.info('Token de registro Push:', token.value);

                    // IMPORTANTE: Salve este token no seu banco de dados (Firestore)
                    // associado ao documento do usuário atual.
                    // O seu backend usará este token para enviar notificações para este aparelho.
                    if (App.state.currentUser) {
                        try {
                            await App.data.updateDocument('users', App.state.currentUser.uid, { fcmToken: token.value });
                            console.log("FCM token salvo para o usuário.");
                        } catch (error) {
                            console.error("Erro ao salvar o FCM token:", error);
                        }
                    }
                });

                // Disparado em caso de erro no registro
                PushNotifications.addListener('registrationError', (err) => {
                    console.error('Erro no registro Push:', err);
                });

                // Disparado quando uma notificação é recebida com o app em primeiro plano
                PushNotifications.addListener('pushNotificationReceived', (notification) => {
                    console.log('Notificação Push recebida:', notification);
                    // Exibe um alerta para o usuário, já que a notificação não aparece
                    // na barra de status quando o app está aberto.
                    App.ui.showAlert(
                        `${notification.title}: ${notification.body}`,
                        'info',
                        5000
                    );
                });

                // Disparado quando o usuário toca na notificação (com o app fechado ou em segundo plano)
                PushNotifications.addListener('pushNotificationActionPerformed', (notification) => {
                    console.log('Ação de Notificação Push executada:', notification);
                    // Aqui você pode redirecionar o usuário para uma tela específica
                    // com base nos dados da notificação.
                    // Ex: if (notification.notification.data.goToPage) { ... }
                });
            }
        },
        
        auth: {
            async checkSession() {
                onAuthStateChanged(auth, async (user) => {
                    if (user) {
                        App.ui.setLoading(true, "A carregar dados do utilizador...");
                        const userDoc = await App.data.getUserData(user.uid);

                        if (userDoc && userDoc.active) {
                            let companyDoc = null;
                            // [CORREÇÃO] Se for super-admin, o companyId deve ser ignorado para acesso global.
                            if (userDoc.role === 'super-admin') {
                                delete userDoc.companyId; // Garante que a sessão do super-admin não fique presa a uma empresa.
                            }

                            // Bloqueia o login se a empresa do utilizador estiver inativa
                            if (userDoc.role !== 'super-admin' && userDoc.companyId) {
                                companyDoc = await App.data.getDocument('companies', userDoc.companyId);
                                if (!companyDoc || companyDoc.active === false) {
                                    App.auth.logout();
                                    App.ui.showLoginMessage("A sua empresa está desativada. Por favor, contate o suporte.", "error");
                                    return;
                                }
                            }

                            App.state.currentUser = { ...user, ...userDoc };

                            // Validação CRÍTICA para o modelo multi-empresa
                            if (!App.state.currentUser.companyId && App.state.currentUser.role !== 'super-admin') {
                                App.auth.logout();
                                App.ui.showLoginMessage("A sua conta não está associada a uma empresa. Contacte o suporte.", "error");
                                return;
                            }

                            // **FIX DA CORRIDA DE DADOS**: Carrega os dados essenciais ANTES de renderizar a tela.
                            App.ui.setLoading(true, "A carregar configurações...");
                            try {
                                // 1. Carregar configurações globais
                                const globalConfigsDoc = await getDoc(doc(db, 'global_configs', 'main'));
                                if (globalConfigsDoc.exists()) {
                                    App.state.globalConfigs = globalConfigsDoc.data();
                                } else {
                                    console.warn("Documento de configurações globais 'main' não encontrado.");
                                    App.state.globalConfigs = {};
                                }

                                // 2. Pré-popular os dados da empresa (se já foram carregados)
                                if (companyDoc) {
                                    App.state.companies = [companyDoc];
                                }

                                // 3. Agora é seguro mostrar a tela principal
                                App.actions.saveUserProfileLocally(App.state.currentUser);
                                App.ui.showAppScreen(); // A renderização do menu aqui agora terá os dados necessários
                                App.data.listenToAllData(); // Inicia os ouvintes para atualizações em tempo real

                                const draftRestored = await App.actions.checkForDraft();
                                if (!draftRestored) {
                                    const lastTab = localStorage.getItem('agrovetor_lastActiveTab');
                                    App.ui.showTab(lastTab || 'dashboard');
                                }

                                if (navigator.onLine) {
                                    App.actions.syncOfflineWrites();
                                }

                            } catch (error) {
                                console.error("Falha crítica ao carregar dados iniciais:", error);
                                App.auth.logout();
                                App.ui.showLoginMessage("Não foi possível carregar as configurações da aplicação. Tente novamente.", "error");
                            }

                        } else {
                            this.logout();
                            App.ui.showLoginMessage("A sua conta foi desativada ou não foi encontrada.");
                        }
                    } else {
                        const localProfiles = App.actions.getLocalUserProfiles();
                        if (localProfiles.length > 0 && !navigator.onLine) {
                            App.ui.showOfflineUserSelection();
                        } else {
                            App.ui.showLoginScreen();
                        }
                    }
                    App.ui.setLoading(false);
                });
            },

            async login() {
                const email = App.elements.loginUser.value.trim();
                const password = App.elements.loginPass.value;
                if (!email || !password) {
                    App.ui.showLoginMessage("Preencha e-mail e senha.");
                    return;
                }
                App.ui.setLoading(true, "A autenticar...");
                try {
                    // Define a persistência da sessão para 'session', que limpa ao fechar o browser/app.
                    await setPersistence(auth, browserSessionPersistence);
                    await signInWithEmailAndPassword(auth, email, password);
                } catch (error) {
                    if (error.code === 'auth/user-not-found' || error.code === 'auth/wrong-password' || error.code === 'auth/invalid-credential') {
                        App.ui.showLoginMessage("E-mail ou senha inválidos.");
                    } else if (error.code === 'auth/network-request-failed') {
                        App.ui.showLoginMessage("Erro de rede. Verifique sua conexão e tente novamente.");
                    } else {
                        App.ui.showLoginMessage("Ocorreu um erro ao fazer login.");
                    }
                    console.error("Erro de login:", error.code, error.message);
                    // Apenas para o loading em caso de erro. Em caso de sucesso, a checkSession cuidará disso.
                    App.ui.setLoading(false);
                }
            },
            async loginOffline(email, password) {
                if (!email || !password) {
                    App.ui.showAlert("Por favor, insira e-mail e senha.", "warning");
                    return;
                }

                try {
                    const credentials = await OfflineDB.get('offline-credentials', email.toLowerCase());

                    if (!credentials) {
                        App.ui.showAlert("Credenciais offline não encontradas para este e-mail. Faça login online primeiro e habilite o acesso offline.", "error");
                        return;
                    }

                    const hashedPassword = CryptoJS.PBKDF2(password, credentials.salt, {
                        keySize: 256 / 32,
                        iterations: 1000
                    }).toString();

                    if (hashedPassword === credentials.hashedPassword) {
                        App.state.currentUser = credentials.userProfile;

                        App.ui.setLoading(true, "A carregar dados offline...");
                        try {
                            const companyId = App.state.currentUser.companyId;

                            // Pré-carrega os dados da empresa a partir do cache offline
                            if (companyId) {
                                const companyDoc = await App.data.getDocument('companies', companyId);
                                if (companyDoc) {
                                     App.state.companies = [companyDoc];
                                } else {
                                    console.warn("Documento da empresa não encontrado no cache offline durante o login.");
                                }
                            }

                            // Pré-carrega as configurações globais a partir do cache offline
                            const globalConfigsDoc = await getDoc(doc(db, 'global_configs', 'main'));
                            if (globalConfigsDoc.exists()) {
                                App.state.globalConfigs = globalConfigsDoc.data();
                            } else {
                                console.warn("Configurações globais não encontradas no cache offline durante o login.");
                            }

                            // Agora, com os dados essenciais pré-carregados, mostra a tela da aplicação
                            App.ui.showAppScreen();
                            App.mapModule.loadOfflineShapes();
                            App.data.listenToAllData(); // Configura os 'listeners' para futuras atualizações quando estiver online

                        } catch (error) {
                            console.error("Erro ao pré-carregar dados do cache offline:", error);
                            // Fallback para o comportamento antigo se o pré-carregamento falhar
                            App.ui.showAppScreen();
                            App.mapModule.loadOfflineShapes();
                            App.data.listenToAllData();
                        } finally {
                            App.ui.setLoading(false);
                        }
                    } else {
                        App.ui.showAlert("Senha offline incorreta.", "error");
                    }
                } catch (error) {
                    App.ui.showAlert("Ocorreu um erro durante o login offline.", "error");
                    console.error("Erro no login offline:", error);
                }
            },
            async logout() {
                if (navigator.onLine) {
                    await signOut(auth);
                }
                // Limpa todos os listeners e processos em segundo plano
                App.data.cleanupListeners();
                App.actions.stopGpsTracking();
                App.actions.stopAutoSync(); // Para a sincronização automática
                App.charts.destroyAll(); // Destrói todas as instâncias de gráficos

                // Limpa completamente o estado da aplicação para evitar "déjà vu"
                App.state.isImpersonating = false;
                App.state.originalUser = null;
                App.state.currentUser = null;
                App.state.users = [];
                App.state.companies = [];
                App.state.globalConfigs = {};
                App.state.companyConfig = {};
                App.state.registros = [];
                App.state.perdas = [];
                App.state.cigarrinha = [];
                App.state.planos = [];
                App.state.fazendas = [];
                App.state.personnel = [];
                App.state.frentesDePlantio = [];
                App.state.apontamentosPlantio = [];
                App.state.companyLogo = null;
                App.state.harvestPlans = [];
                App.state.activeHarvestPlan = null;
                App.state.armadilhas = [];
                App.state.geoJsonData = null;
                App.state.selectedMapFeature = null;
                App.state.trapNotifications = [];
                App.state.unreadNotificationCount = 0;
                App.state.notifiedTrapIds = new Set();
                App.state.riskViewActive = false;
                App.state.plantio = [];
                App.state.clima = [];
                App.state.apontamentoPlantioFormIsDirty = false;

                // Limpa timers de inatividade e armazenamento local
                clearTimeout(App.state.inactivityTimer);
                clearTimeout(App.state.inactivityWarningTimer);
                localStorage.removeItem('agrovetor_lastActiveTab');
                sessionStorage.removeItem('notifiedTrapIds');

                // Reavalia a sessão para mostrar a tela de login correta (online/offline)
                this.checkSession();
            },
            initiateUserCreation() {
                const els = App.elements.users;
                const email = els.username.value.trim();
                const password = els.password.value;
                const role = els.role.value;
                if (!email || !password) { App.ui.showAlert("Preencha e-mail e senha.", "error"); return; }

                const permissions = {};
                els.permissionsContainer.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                    permissions[cb.dataset.permission] = cb.checked;
                });

                // Define the action to be executed upon confirmation
                const userCreationAction = async () => {
                    let targetCompanyId = App.state.currentUser.companyId;
                    if (App.state.currentUser.role === 'super-admin') {
                        targetCompanyId = App.elements.users.adminTargetCompanyUsers.value;
                        if (!targetCompanyId) {
                            throw new Error("Como Super Admin, você deve selecionar uma empresa alvo para criar o utilizador.");
                        }
                    }

                    const userCredential = await createUserWithEmailAndPassword(secondaryAuth, email, password);
                    const newUser = userCredential.user;
                    await signOut(secondaryAuth);

                    const userData = {
                        username: email.split('@')[0], email, role, active: true, permissions, companyId: targetCompanyId
                    };
                    await App.data.createUserData(newUser.uid, userData);
                    
                    App.ui.showAlert(`Utilizador ${email} criado com sucesso!`);
                    els.username.value = '';
                    els.password.value = '';
                    els.role.value = 'user';
                    App.ui.updatePermissionsForRole('user');
                };

                // Store the action and show the modal
                App.state.adminAction = userCreationAction;
                App.ui.showAdminPasswordConfirmModal();
            },

            async executeAdminAction() {
                const adminPassword = App.elements.adminPasswordConfirmModal.passwordInput.value;
                if (!App.state.adminAction || typeof App.state.adminAction !== 'function') { return; }

                // Se estiver offline, confia no papel do utilizador já logado
                if (!navigator.onLine) {
                    const userRole = App.state.currentUser?.role;
                    if (userRole === 'admin' || userRole === 'super-admin') {
                        App.ui.setLoading(true, "A executar ação offline...");
                        try {
                            await App.state.adminAction();
                            App.ui.closeAdminPasswordConfirmModal();
                        } catch (error) {
                            App.ui.showAlert(`Erro ao executar ação offline: ${error.message}`, "error");
                        } finally {
                            App.state.adminAction = null;
                            App.elements.adminPasswordConfirmModal.passwordInput.value = '';
                            App.ui.setLoading(false);
                        }
                        return;
                    }
                }

                // Fluxo online normal com verificação de senha
                if (!adminPassword) { App.ui.showAlert("Por favor, insira a sua senha de administrador para confirmar.", "error"); return; }
                App.ui.setLoading(true, "A autenticar e executar ação...");

                try {
                    const adminUser = auth.currentUser;
                    const credential = EmailAuthProvider.credential(adminUser.email, adminPassword);
                    await reauthenticateWithCredential(adminUser, credential);

                    // Se a reautenticação for bem-sucedida, executa a ação armazenada
                    await App.state.adminAction();
                    App.ui.closeAdminPasswordConfirmModal();

                } catch (error) {
                    if (error.code === 'auth/wrong-password' || error.code === 'auth/invalid-credential' || error.code === 'auth/invalid-login-credentials') {
                        App.ui.showAlert("A sua senha de administrador está incorreta.", "error");
                    } else if (error.code === 'auth/email-already-in-use') {
                        App.ui.showAlert("Este e-mail já está em uso por outro utilizador.", "error");
                    } else if (error.code === 'auth/weak-password') {
                        App.ui.showAlert("A senha do novo utilizador deve ter pelo menos 6 caracteres.", "error");
                    } else {
                        App.ui.showAlert(`Erro ao executar ação: ${error.message}`, "error");
                        console.error("Erro na ação de administrador:", error);
                    }
                } finally {
                    App.state.adminAction = null; // Limpa a ação após a execução
                    App.elements.adminPasswordConfirmModal.passwordInput.value = '';
                    App.ui.setLoading(false);
                }
            },
            async deleteUser(userId) {
                const userToDelete = App.state.users.find(u => u.id === userId);
                if (!userToDelete) return;
                
                App.ui.showConfirmationModal(`Tem a certeza que deseja EXCLUIR o utilizador ${userToDelete.username}? Esta ação não pode ser desfeita.`, async () => {
                    try {
                        await App.data.updateDocument('users', userId, { active: false });
                        App.actions.removeUserProfileLocally(userId);
                        App.ui.showAlert(`Utilizador ${userToDelete.username} desativado.`);
                        App.ui.closeUserEditModal();
                    } catch (error) {
                        App.ui.showAlert("Erro ao desativar utilizador.", "error");
                    }
                });
            },
            async toggleUserStatus(userId) {
                const user = App.state.users.find(u => u.id === userId);
                if (!user) return;
                const newStatus = !user.active;
                await App.data.updateDocument('users', userId, { active: newStatus });
                App.ui.showAlert(`Utilizador ${user.username} ${newStatus ? 'ativado' : 'desativado'}.`);
            },
            async resetUserPassword(userId) {
                const user = App.state.users.find(u => u.id === userId);
                if (!user || !user.email) return;

                App.ui.showConfirmationModal(`Deseja enviar um e-mail de redefinição de senha para ${user.email}?`, async () => {
                    try {
                        await sendPasswordResetEmail(auth, user.email);
                        App.ui.showAlert(`E-mail de redefinição enviado para ${user.email}.`, 'success');
                    } catch (error) {
                        App.ui.showAlert("Erro ao enviar e-mail de redefinição.", "error");
                        console.error(error);
                    }
                });
            },
            async saveUserChanges(userId) {
                const modalEls = App.elements.userEditModal;
                const role = modalEls.role.value;
                const permissions = {};
                modalEls.permissionGrid.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                    permissions[cb.dataset.permission] = cb.checked;
                });
                
                await App.data.updateDocument('users', userId, { role, permissions });
                App.ui.showAlert("Alterações guardadas com sucesso!");
                App.ui.closeUserEditModal();
            }
        },

        data: {
            cleanupListeners() {
                App.state.unsubscribeListeners.forEach(unsubscribe => unsubscribe());
                App.state.unsubscribeListeners = [];
            },
            listenToAllData() {
                this.cleanupListeners();

                // Ouve as configurações globais para TODOS os utilizadores
                const globalConfigsRef = doc(db, 'global_configs', 'main');
                const unsubscribeGlobalConfigs = onSnapshot(globalConfigsRef, (doc) => {
                    if (doc.exists()) {
                        App.state.globalConfigs = doc.data();
                    } else {
                        console.warn("Documento de configurações globais 'main' não encontrado. Recursos podem estar desativados por padrão.");
                        App.state.globalConfigs = {}; // Garante que é um objeto vazio
                    }
                    // Re-renderiza o menu sempre que as flags globais mudam
                    App.ui.renderMenu();
                }, (error) => {
                    console.error("Erro ao ouvir as configurações globais: ", error);
                    App.state.globalConfigs = {}; // Reseta em caso de erro
                    App.ui.renderMenu(); // Re-renderiza o menu com flags desativadas
                });
                App.state.unsubscribeListeners.push(unsubscribeGlobalConfigs);

                const companyId = App.state.currentUser.companyId;
                const isSuperAdmin = App.state.currentUser.role === 'super-admin';

                const companyScopedCollections = ['users', 'fazendas', 'personnel', 'registros', 'perdas', 'planos', 'harvestPlans', 'armadilhas', 'cigarrinha', 'cigarrinhaAmostragem', 'frentesDePlantio', 'apontamentosPlantio', 'clima'];

                if (isSuperAdmin) {
                    // Super Admin ouve TODOS os dados de todas as coleções relevantes
                    companyScopedCollections.forEach(collectionName => {
                        const q = collection(db, collectionName); // Sem filtro 'where'
                        const unsubscribe = onSnapshot(q, (querySnapshot) => {
                            const data = [];
                            querySnapshot.forEach((doc) => data.push({ id: doc.id, ...doc.data() }));
                            App.state[collectionName] = data;
                            App.ui.renderSpecificContent(collectionName);
                        }, (error) => {
                            console.error(`Erro ao ouvir a coleção ${collectionName} como Super Admin: `, error);
                        });
                        App.state.unsubscribeListeners.push(unsubscribe);
                    });

                    // Super Admin também ouve a coleção de empresas
                    const qCompanies = collection(db, 'companies');
                    const unsubscribeCompanies = onSnapshot(qCompanies, (querySnapshot) => {
                        const data = [];
                        querySnapshot.forEach((doc) => data.push({ id: doc.id, ...doc.data() }));
                        App.state['companies'] = data;
                        App.ui.renderSpecificContent('companies');
                    }, (error) => console.error(`Erro ao ouvir a coleção companies: `, error));
                    App.state.unsubscribeListeners.push(unsubscribeCompanies);

                    App.state.companyLogo = null;
                    App.ui.renderLogoPreview();

                } else if (companyId) {
                    // Utilizador normal ouve apenas os dados da sua própria empresa
                    companyScopedCollections.forEach(collectionName => {
                        const q = query(collection(db, collectionName), where("companyId", "==", companyId));
                        const unsubscribe = onSnapshot(q, (querySnapshot) => {
                            const data = [];
                            querySnapshot.forEach((doc) => data.push({ id: doc.id, ...doc.data() }));
                            App.state[collectionName] = data;

                            if (collectionName === 'armadilhas') {
                                if (App.state.mapboxMap) App.mapModule.loadTraps();
                                App.mapModule.checkTrapStatusAndNotify();
                            }
                            App.ui.renderSpecificContent(collectionName);
                        }, (error) => {
                            console.error(`Erro ao ouvir a coleção ${collectionName}: `, error);
                        });
                        App.state.unsubscribeListeners.push(unsubscribe);
                    });

                    // **NOVO**: Ouvir o documento da própria empresa para obter os módulos subscritos
                    const companyDocRef = doc(db, 'companies', companyId);
                    const unsubscribeCompany = onSnapshot(companyDocRef, (doc) => {
                        if (doc.exists()) {
                            // Coloca a empresa do utilizador no estado, para que o menu possa ser renderizado corretamente
                            App.state.companies = [{ id: doc.id, ...doc.data() }];
                        } else if (navigator.onLine) {
                            // Se estiver online e a empresa não for encontrada, desloga o utilizador por segurança.
                            console.error(`Empresa com ID ${companyId} não encontrada. A deslogar o utilizador.`);
                            App.auth.logout();
                        } else {
                            // Se estiver offline e o documento da empresa não estiver no cache, permite que a aplicação continue.
                            // Os módulos podem não ser renderizados corretamente, mas o acesso não é bloqueado.
                            console.warn(`Documento da empresa com ID ${companyId} não encontrado no cache offline. O menu pode estar incompleto.`);
                        }
                        App.ui.renderMenu(); // Re-renderiza o menu quando os dados da empresa mudam
                    });
                    App.state.unsubscribeListeners.push(unsubscribeCompany);

                    // Configurações específicas da empresa (logotipo, etc.)
                    const configDocRef = doc(db, 'config', companyId);
                    const unsubscribeConfig = onSnapshot(configDocRef, (doc) => {
                        if (doc.exists()) {
                            const configData = doc.data();
                            App.state.companyConfig = configData; // Carrega todas as configurações da empresa
                            App.state.companyLogo = configData.logoBase64 || null;

                            // Atualiza a UI com o valor carregado
                            const cigarrinhaMethodSelect = document.getElementById('cigarrinhaCalcMethod');
                            if (cigarrinhaMethodSelect) {
                                cigarrinhaMethodSelect.value = configData.cigarrinhaCalcMethod || '5';
                            }

                            if (configData.shapefileURL) {
                                App.mapModule.loadAndCacheShapes(configData.shapefileURL);
                            }

                        } else {
                            App.state.companyLogo = null;
                            App.state.companyConfig = {};
                        }
                        App.ui.renderLogoPreview();
                    });
                    App.state.unsubscribeListeners.push(unsubscribeConfig);
                } else {
                    console.error("Utilizador não é Super Admin e não tem companyId. Carregamento de dados bloqueado.");
                }
            },
            async getDocument(collectionName, docId, options) {
                return await getDoc(doc(db, collectionName, docId)).then(docSnap => {
                    return docSnap.exists() ? { id: docSnap.id, ...docSnap.data() } : null;
                });
            },
            async addDocument(collectionName, data) {
                return await addDoc(collection(db, collectionName), { ...data, createdAt: serverTimestamp() });
            },
            async setDocument(collectionName, docId, data) {
                return await setDoc(doc(db, collectionName, docId), data, { merge: true });
            },
            async updateDocument(collectionName, docId, data) {
                return await updateDoc(doc(db, collectionName, docId), data);
            },
            async deleteDocument(collectionName, docId) {
                return await deleteDoc(doc(db, collectionName, docId));
            },
            async getUserData(uid, options = {}) {
                return this.getDocument('users', uid, options);
            },
            async createUserData(uid, data) {
                return this.setDocument('users', uid, data);
            },
        },
        
        ui: {
            _getThemeColors() {
                const styles = getComputedStyle(document.documentElement);
                return {
                    primary: styles.getPropertyValue('--color-primary').trim(),
                    primaryLight: styles.getPropertyValue('--color-primary-light').trim(),
                    text: styles.getPropertyValue('--color-text').trim(),
                    border: styles.getPropertyValue('--color-border').trim(),
                };
            },
            setLoading(isLoading, progressText = "A processar...") {
                App.elements.loadingOverlay.style.display = isLoading ? 'flex' : 'none';
                App.elements.loadingProgressText.textContent = progressText;
            },
            showLoginScreen() {
                App.elements.loginForm.style.display = 'block';
                App.elements.offlineUserSelection.style.display = 'none';
                App.elements.loginScreen.style.display = 'flex';
                App.elements.appScreen.style.display = 'none';
                
                if (App.elements.userMenu && App.elements.userMenu.container) {
                    App.elements.userMenu.container.style.display = 'none';
                }
                if (App.elements.notificationBell && App.elements.notificationBell.container) {
                    App.elements.notificationBell.container.style.display = 'none';
                }

                App.elements.loginUser.value = '';
                App.elements.loginPass.value = '';
                App.elements.loginUser.focus();
                this.closeAllMenus();
                App.ui.setLoading(false);
            },
            showOfflineUserSelection() { // Removed profiles argument
                App.elements.loginForm.style.display = 'none';
                App.elements.offlineUserSelection.style.display = 'block';
                // No longer need to populate a select list
                const offlineEmailInput = document.getElementById('offlineEmail');
                if(offlineEmailInput) {
                    offlineEmailInput.value = ''; // Clear previous entries
                    offlineEmailInput.focus();
                }
                App.elements.loginScreen.style.display = 'flex';
                App.elements.appScreen.style.display = 'none';
                App.ui.setLoading(false);
            },
            showAppScreen() {
                const { currentUser } = App.state;
                App.ui.setLoading(false);
                App.elements.loginScreen.style.display = 'none';
                App.elements.appScreen.style.display = 'flex';
                App.elements.userMenu.container.style.display = 'block';
                App.elements.notificationBell.container.style.display = 'block';
                App.elements.userMenu.username.textContent = currentUser.username || currentUser.email;
                
                // ALTERAÇÃO PONTO 3: Alterar título do cabeçalho
                App.elements.headerTitle.innerHTML = `<i class="fas fa-leaf"></i> AgroVetor`;

                this.updateDateTime();
                setInterval(() => this.updateDateTime(), 60000);

                // Adiciona verificação periódica para o status das armadilhas
                setInterval(() => {
                    if (App.state.armadilhas.length > 0) {
                        App.mapModule.checkTrapStatusAndNotify();
                    }
                }, 60000); // Verifica a cada minuto

                this.renderMenu();
                this.renderAllDynamicContent();
                App.actions.resetInactivityTimer();
                App.actions.loadNotificationHistory(); // Carrega o histórico de notificações
                App.mapModule.initMap(); // INICIALIZA O MAPA AQUI
                App.actions.startGpsTracking(); // O rastreamento agora é manual
                App.actions.startAutoSync(); // Inicia a sincronização automática
            },
            renderSpecificContent(collectionName) {
                const activeTab = document.querySelector('.tab-content.active')?.id;

                switch (collectionName) {
                    case 'companies':
                        if (activeTab === 'gerenciarEmpresas') {
                            this.renderCompaniesList();
                        }
                        break;
                    case 'users':
                        this.populateUserSelects([App.elements.planejamento.responsavel]);
                        if (activeTab === 'gerenciarUsuarios') {
                            this.renderUsersList();
                        }
                        if (App.elements.historyFilterModal.overlay.classList.contains('show')) {
                             this.populateUserSelects([App.elements.historyFilterModal.userSelect]);
                        }
                        break;
                    case 'fazendas':
                        this.populateFazendaSelects();
                        if (activeTab === 'cadastros') {
                            this.renderFarmSelect();
                        }
                        break;
                    case 'personnel':
                        this.populateOperatorSelects();
                        if (activeTab === 'cadastrarPessoas') {
                            this.renderPersonnelList();
                        }
                        break;
                    case 'frentesDePlantio':
                        if (activeTab === 'frenteDePlantio') {
                            this.renderFrenteDePlantioList();
                        }
                        this.populateFrenteDePlantioSelect();
                        break;
                    case 'apontamentosPlantio':
                        // This collection is for storing data, no direct render action needed on snapshot
                        break;
                    case 'planos':
                        if (activeTab === 'planejamento') {
                            this.renderPlanejamento();
                        }
                        break;
                    case 'harvestPlans':
                        this.populateHarvestPlanSelect();
                        if (activeTab === 'planejamentoColheita') {
                            this.showHarvestPlanList();
                        }
                        break;
                    case 'registros':
                        if (activeTab === 'dashboard' && document.getElementById('dashboard-broca').style.display !== 'none') {
                            App.charts.renderBrocaDashboardCharts();
                        }
                        if (activeTab === 'excluirDados') {
                            this.renderExclusao();
                        }
                        break;
                    case 'perdas':
                        if (activeTab === 'dashboard' && document.getElementById('dashboard-perda').style.display !== 'none') {
                            App.charts.renderPerdaDashboardCharts();
                        }
                        if (activeTab === 'excluirDados') {
                            this.renderExclusao();
                        }
                        break;
                    // No specific actions needed for 'cigarrinha' or 'armadilhas' on snapshot,
                    // as their primary UIs are user-triggered or handled elsewhere.
                }
            },

            renderAllDynamicContent() {
                const renderWithCatch = (name, fn) => {
                    try {
                        fn();
                    } catch (error) {
                        console.error(`Error rendering component: ${name}`, error);
                        // Optionally, display a message to the user in the specific component's area
                    }
                };

                renderWithCatch('populateFazendaSelects', () => this.populateFazendaSelects());
                renderWithCatch('populateUserSelects', () => this.populateUserSelects([App.elements.planejamento.responsavel]));
                renderWithCatch('populateOperatorSelects', () => this.populateOperatorSelects());
                renderWithCatch('renderUsersList', () => this.renderUsersList());
                renderWithCatch('renderPersonnelList', () => this.renderPersonnelList());
                renderWithCatch('renderFrenteDePlantioList', () => this.renderFrenteDePlantioList());
                renderWithCatch('populateFrenteDePlantioSelect', () => this.populateFrenteDePlantioSelect());
                renderWithCatch('renderLogoPreview', () => this.renderLogoPreview());
                renderWithCatch('renderPlanejamento', () => this.renderPlanejamento());
                renderWithCatch('showHarvestPlanList', () => this.showHarvestPlanList());
                renderWithCatch('populateHarvestPlanSelect', () => this.populateHarvestPlanSelect());

                renderWithCatch('dashboard-view', () => {
                    if (document.getElementById('dashboard').classList.contains('active')) {
                        this.showDashboardView('broca');
                    }
                });
            },
            showLoginMessage(message) { App.elements.loginMessage.textContent = message; },
            showAlert(message, type = 'success', duration = 3000) {
                const { alertContainer } = App.elements;
                if (!alertContainer) return;
                const icons = { success: 'check-circle', error: 'exclamation-circle', warning: 'info-circle', info: 'info-circle' };
                alertContainer.innerHTML = `<i class="fas fa-${icons[type] || 'info-circle'}"></i> ${message}`;
                alertContainer.className = `show ${type}`;
                setTimeout(() => alertContainer.classList.remove('show'), duration);

                // Adicionado para salvar a notificação
                const notification = {
                    title: type.charAt(0).toUpperCase() + type.slice(1), // ex: "Success"
                    message: message,
                    type: type,
                    timestamp: new Date()
                };
                App.actions.saveNotification(notification);
            },

            showSystemNotification(title, message, type = 'info', options = {}) {
                const { list, count, noNotifications } = App.elements.notificationBell;
                const { logId = null } = options;

                const newNotification = {
                    title: title,
                    type: type,
                    message: message,
                    timestamp: new Date(),
                    logId: logId // Adiciona o ID do log, se disponível
                };

                // Adiciona a nova notificação ao início da lista
                App.state.trapNotifications.unshift(newNotification);
                App.state.unreadNotificationCount++;

                this.updateNotificationBell();
                App.actions.saveNotification(newNotification); // Salva a notificação completa
            },
            updateDateTime() { App.elements.currentDateTime.innerHTML = `<i class="fas fa-clock"></i> ${new Date().toLocaleString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' })}`; },
            renderMenu() {
                const { menu } = App.elements; const { menuConfig } = App.config; const { currentUser } = App.state;
                menu.innerHTML = '';
                const menuContent = document.createElement('div');
                menuContent.className = 'menu-content';
                menu.appendChild(menuContent);

                const createMenuItem = (item) => {
                    const { currentUser, companies } = App.state;
                    const isSuperAdmin = currentUser.role === 'super-admin';

                    const hasPermission = isSuperAdmin || (item.submenu ?
                        item.submenu.some(sub => currentUser.permissions && currentUser.permissions[sub.permission]) :
                        (currentUser.permissions && currentUser.permissions[item.permission]));

                    if (!hasPermission) return null;

                    if (!isSuperAdmin) {
                        const userCompany = companies.find(c => c.id === currentUser.companyId);
                        const subscribedModules = new Set(userCompany?.subscribedModules || []);

                        const isVisible = item.submenu ?
                            item.submenu.some(sub => App.isFeatureGloballyActive(sub.permission) && subscribedModules.has(sub.permission)) :
                            (App.isFeatureGloballyActive(item.permission) && subscribedModules.has(item.permission));

                        if (!isVisible) return null;
                    }
                    
                    const btn = document.createElement('button');
                    btn.className = 'menu-btn';
                    btn.innerHTML = `<i class="${item.icon}"></i> <span>${item.label}</span>`;

                    if (isSuperAdmin) {
                        const isAnySubItemHidden = item.submenu && item.submenu.some(sub => !App.isFeatureGloballyActive(sub.permission));
                        const isDirectItemHidden = !item.submenu && item.permission && !App.isFeatureGloballyActive(item.permission);

                        if (isAnySubItemHidden || isDirectItemHidden) {
                            btn.classList.add('globally-disabled-feature');
                            btn.innerHTML += '<span class="feature-status-badge">Oculto</span>';
                        }
                    }
                    
                    if (item.submenu) {
                        btn.innerHTML += '<span class="arrow">&rsaquo;</span>';
                        btn.addEventListener('click', (e) => {
                            e.stopPropagation();
                            this.renderSubmenu(item);
                        });
                    } else {
                        btn.addEventListener('click', () => {
                            this.closeAllMenus();
                            this.showTab(item.target);
                        });
                    }
                    return btn;
                };
                menuConfig.forEach(item => { const menuItem = createMenuItem(item); if (menuItem) menuContent.appendChild(menuItem); });
            },
            renderSubmenu(parentItem) {
                const { menu } = App.elements;
                let submenuContent = menu.querySelector('.submenu-content');
                if (submenuContent) submenuContent.remove();

                submenuContent = document.createElement('div');
                submenuContent.className = 'submenu-content';

                const backBtn = document.createElement('button');
                backBtn.className = 'submenu-back-btn';
                backBtn.innerHTML = '<i class="fas fa-arrow-left"></i> <span>Voltar</span>';
                backBtn.onclick = () => {
                    submenuContent.classList.remove('active');
                    setTimeout(() => this.renderMenu(), 300);
                };
                submenuContent.appendChild(backBtn);
                
                const { currentUser, companies } = App.state;
                const userCompany = currentUser.role !== 'super-admin' ? companies.find(c => c.id === currentUser.companyId) : null;
                const subscribedModules = new Set(userCompany?.subscribedModules || []);

                parentItem.submenu.forEach(subItem => {
                    const isSuperAdmin = currentUser.role === 'super-admin';
                    const hasPermission = isSuperAdmin || (currentUser.permissions && currentUser.permissions[subItem.permission]);

                    if (!hasPermission) return;

                    const isGloballyActive = App.isFeatureGloballyActive(subItem.permission);
                    const isSubscribed = isSuperAdmin || subscribedModules.has(subItem.permission);

                    if (!isSuperAdmin && (!isGloballyActive || !isSubscribed)) {
                        return; // Não renderiza para utilizadores normais se não estiver globalmente ativo OU não estiver subscrito
                    }

                    const subBtn = document.createElement('button');
                    subBtn.className = 'submenu-btn';
                    subBtn.innerHTML = `<i class="${subItem.icon}"></i> ${subItem.label}`;

                    if (isSuperAdmin && !isGloballyActive) {
                        subBtn.classList.add('globally-disabled-feature');
                        subBtn.innerHTML += '<span class="feature-status-badge">Oculto</span>';
                    }

                    if (!isSubscribed && !isSuperAdmin) {
                        // Este caso não deveria acontecer por causa do filtro acima, mas é uma segurança.
                        subBtn.classList.add('disabled-module');
                        subBtn.title = "Módulo não disponível na sua subscrição.";
                        subBtn.addEventListener('click', (e) => {
                            e.stopPropagation();
                            App.ui.showAlert("Este módulo não está incluído na subscrição da sua empresa.", "warning", 5000);
                        });
                    } else {
                        subBtn.addEventListener('click', () => {
                            this.closeAllMenus();
                            this.showTab(subItem.target);
                        });
                    }
                    submenuContent.appendChild(subBtn);
                });
                menu.appendChild(submenuContent);
                requestAnimationFrame(() => submenuContent.classList.add('active'));
            },
            closeAllMenus() {
                document.body.classList.remove('mobile-menu-open');
                App.elements.menu.classList.remove('open');
                App.elements.btnToggleMenu.classList.remove('open');
                const activeSubmenu = App.elements.menu.querySelector('.submenu-content.active');
                if(activeSubmenu) activeSubmenu.classList.remove('active');
            },
            populateHarvestPlanSelect() {
                const { select } = App.elements.relatorioColheita;
                const savedValue = select.value;
                select.innerHTML = '<option value="">Selecione um plano de colheita...</option>';
                if (App.state.harvestPlans.length === 0) {
                    select.innerHTML += '<option value="" disabled>Nenhum plano salvo encontrado</option>';
                } else {
                    App.state.harvestPlans.forEach(plan => {
                        select.innerHTML += `<option value="${plan.id}">${plan.frontName}</option>`;
                    });
                }
                select.value = savedValue;
            },
            showTab(id) {
                const { currentUser, companies } = App.state;

                // Encontrar o item de menu correspondente para obter a permissão necessária
                let requiredPermission = null;
                App.config.menuConfig.forEach(item => {
                    if (item.target === id) {
                        requiredPermission = item.permission;
                    } else if (item.submenu) {
                        const subItem = item.submenu.find(sub => sub.target === id);
                        if (subItem) {
                            requiredPermission = subItem.permission;
                        }
                    }
                });

                // LÓGICA DE BLOQUEIO REFINADA
                if (requiredPermission && currentUser.role !== 'super-admin' && !App.state.isImpersonating) {
                    const isGloballyActive = App.isFeatureGloballyActive(requiredPermission);
                    if (!isGloballyActive) {
                        App.ui.showAlert("Esta funcionalidade não está ativa no momento.", "info", 5000);
                        return; // Bloqueia a navegação
                    }

                    const userCompany = companies.find(c => c.id === currentUser.companyId);
                    if (!userCompany) {
                        console.warn(`Tentativa de acesso ao módulo ${requiredPermission} sem dados da empresa carregados. A bloquear.`);
                        return;
                    }
                    const subscribedModules = new Set(userCompany?.subscribedModules || []);
                    if (!subscribedModules.has(requiredPermission)) {
                        App.ui.showAlert("Este módulo não está incluído na subscrição da sua empresa.", "warning", 5000);
                        return; // Bloqueia a navegação
                    }
                }


                const currentActiveTab = document.querySelector('.tab-content.active');
                if (currentActiveTab && currentActiveTab.id === 'apontamentoPlantio' && App.state.apontamentoPlantioFormIsDirty && id !== 'apontamentoPlantio') {
                    App.ui.showConfirmationModal(
                        "Você tem alterações não salvas. Deseja descartá-las e sair?",
                        () => { // onConfirm: Discard and Leave
                            App.state.apontamentoPlantioFormIsDirty = false;
                            App.ui.showTab(id); // Re-trigger the navigation now that the flag is clean
                        }
                    );
                    // Customize modal buttons for this specific confirmation
                    const { confirmBtn, cancelBtn } = App.elements.confirmationModal;
                    confirmBtn.textContent = 'Descartar e Sair';
                    cancelBtn.textContent = 'Continuar Editando';
                    cancelBtn.style.display = 'inline-flex';

                    return; // Stop the current navigation attempt
                }

                if (currentActiveTab && currentActiveTab.id !== id) { // Check if we are actually switching tabs
                    if (currentActiveTab.id === 'lancamentoCigarrinha') {
                        App.ui.clearForm(App.elements.cigarrinha.form);
                    }
                    if (currentActiveTab.id === 'lancamentoCigarrinhaAmostragem') {
                        const amostragemEls = App.elements.cigarrinhaAmostragem;
                        App.ui.clearForm(amostragemEls.form);
                        if (amostragemEls.amostrasContainer) {
                            amostragemEls.amostrasContainer.innerHTML = '';
                        }
                        if (amostragemEls.resultado) {
                            amostragemEls.resultado.textContent = '';
                        }
                    }
                    // Limpa o formulário de apontamento de plantio ao sair da aba
                    if (currentActiveTab.id === 'apontamentoPlantio') {
                        const els = App.elements.apontamentoPlantio;
                        App.ui.clearForm(els.form);
                        if (els.recordsContainer) els.recordsContainer.innerHTML = '';
                        if (els.totalArea) els.totalArea.textContent = 'Total de Área Plantada: 0,00 ha';
                        if (els.leaderName) els.leaderName.textContent = '';
                        if (els.entryId) els.entryId.value = ''; // Garante que sai do modo de edição
                        App.ui.setDefaultDatesForEntryForms();
                        App.state.apontamentoPlantioFormIsDirty = false;
                    }
                }

                const mapContainer = App.elements.monitoramentoAereo.container;
                if (id === 'monitoramentoAereo') {
                    mapContainer.classList.add('active');
                    if (App.state.mapboxMap) {
                        // Força o redimensionamento do mapa para o contêiner visível
                        setTimeout(() => App.state.mapboxMap.resize(), 0);
                    }
                } else {
                    mapContainer.classList.remove('active');
                }

                document.querySelectorAll('.tab-content').forEach(tab => {
                    if (tab.id !== 'monitoramentoAereo-container') {
                        tab.classList.remove('active');
                        tab.hidden = true;
                    }
                });

                const tab = document.getElementById(id);
                if (tab) {
                    tab.classList.add('active');
                    tab.hidden = false;
                }
                
                if (id === 'dashboard') {
                   this.showDashboardView('broca'); 
                } else {
                    App.charts.destroyAll(); 
                }
                
                if (id === 'configuracoesEmpresa') {
                    App.actions.setupPlantingGoals();
                }
                if (id === 'syncHistory') this.renderSyncHistory();
                if (id === 'excluirDados') this.renderExclusao();
                if (id === 'gerenciarUsuarios') {
                    this.renderUsersList();
                    this.renderPermissionItems(App.elements.users.permissionsContainer);
                    if (App.state.currentUser.role === 'super-admin') {
                        const { superAdminUserCreation, adminTargetCompanyUsers } = App.elements.users;
                        superAdminUserCreation.style.display = 'block';
                        adminTargetCompanyUsers.innerHTML = '<option value="">Selecione uma empresa...</option>';
                        App.state.companies.sort((a,b) => a.name.localeCompare(b.name)).forEach(c => {
                            adminTargetCompanyUsers.innerHTML += `<option value="${c.id}">${c.name}</option>`;
                        });
                    } else {
                        const superAdminUserCreationEl = document.getElementById('superAdminUserCreation');
                        if (superAdminUserCreationEl) {
                           superAdminUserCreationEl.style.display = 'none';
                        }
                    }
                }
                 if (id === 'gerenciarEmpresas') {
                    this.renderCompaniesList();
                    this.renderCompanyModules('newCompanyModules');
                    this.renderGlobalFeatures(); // NOVO
                }
                if (id === 'cadastros') {
                    this.renderFarmSelect();
                    if (App.state.currentUser.role === 'super-admin') {
                        const { superAdminFarmCreation, adminTargetCompanyFarms } = App.elements.cadastros;
                        superAdminFarmCreation.style.display = 'block';
                        adminTargetCompanyFarms.innerHTML = '<option value="">Selecione uma empresa...</option>';
                        App.state.companies.sort((a,b) => a.name.localeCompare(b.name)).forEach(c => {
                            adminTargetCompanyFarms.innerHTML += `<option value="${c.id}">${c.name}</option>`;
                        });
                    } else {
                        const superAdminFarmCreationEl = document.getElementById('superAdminFarmCreation');
                        if (superAdminFarmCreationEl) {
                            superAdminFarmCreationEl.style.display = 'none';
                        }
                    }
                }
                if (id === 'cadastrarPessoas') this.renderPersonnelList();
                if (id === 'planejamento') this.renderPlanejamento();
                if (id === 'planejamentoColheita') {
                    this.showHarvestPlanList();
                    if (App.state.currentUser.role === 'super-admin') {
                        const { superAdminHarvestCreation, adminTargetCompanyHarvest } = App.elements.harvest;
                        superAdminHarvestCreation.style.display = 'block';
                        adminTargetCompanyHarvest.innerHTML = '<option value="">Selecione uma empresa...</option>';
                        App.state.companies.sort((a, b) => a.name.localeCompare(b.name)).forEach(c => {
                            adminTargetCompanyHarvest.innerHTML += `<option value="${c.id}">${c.name}</option>`;
                        });
                    } else {
                        const superAdminHarvestCreationEl = document.getElementById('superAdminHarvestCreation');
                        if (superAdminHarvestCreationEl) {
                           superAdminHarvestCreationEl.style.display = 'none';
                        }
                    }
                }
                if (['relatorioBroca', 'relatorioPerda', 'relatorioMonitoramento', 'relatorioCigarrinha'].includes(id)) this.setDefaultDatesForReportForms();
                if (id === 'relatorioColheitaCustom') this.populateHarvestPlanSelect();
                if (['lancamentoBroca', 'lancamentoPerda', 'lancamentoCigarrinha', 'apontamentoPlantio'].includes(id)) this.setDefaultDatesForEntryForms();
                
                localStorage.setItem('agrovetor_lastActiveTab', id);
                this.closeAllMenus();
            },

            // ALTERAÇÃO PONTO 4: Nova função para atualizar o sino de notificação
            updateNotificationBell() {
                const { list, count, noNotifications } = App.elements.notificationBell;
                const notifications = App.state.trapNotifications;
                const unreadCount = App.state.unreadNotificationCount;

                list.innerHTML = ''; // Limpa a lista atual

                if (notifications.length === 0) {
                    noNotifications.innerHTML = '<i class="fas fa-bell-slash"></i><p>Nenhuma notificação nova.</p>';
                    noNotifications.style.display = 'flex';
                    list.style.display = 'none';
                } else {
                    noNotifications.style.display = 'none';
                    list.style.display = 'block';

                    notifications.forEach(notif => {
                        const item = document.createElement('div');
                        const timeAgo = this.timeSince(notif.timestamp);

                        let iconClass = 'fa-info-circle';
                        let typeClass = notif.type || 'info';

                        const lowerCaseTitle = (notif.title || '').toLowerCase();
                        if (notif.trapId) {
                            item.dataset.trapId = notif.trapId;
                            iconClass = 'fa-bug';
                        } else if (lowerCaseTitle.includes('sincroniza')) {
                            iconClass = 'fa-sync-alt';
                            if (notif.logId) item.dataset.logId = notif.logId;
                        }

                        const itemTitle = notif.title || (notif.trapId ? 'Armadilha Requer Atenção' : 'Notificação do Sistema');
                        item.className = `notification-item ${typeClass}`;

                        item.innerHTML = `
                            <i class="fas ${iconClass}"></i>
                            <div class="notification-item-content">
                                <p><strong>${itemTitle}</strong></p>
                                <p>${notif.message}</p>
                                <div class="timestamp">${timeAgo}</div>
                            </div>
                        `;
                        list.appendChild(item);
                    });
                }

                if (unreadCount > 0) {
                    count.textContent = unreadCount;
                    count.classList.add('visible');
                } else {
                    count.classList.remove('visible');
                }
            },

            timeSince(date) {
                const seconds = Math.floor((new Date() - date) / 1000);
                let interval = seconds / 31536000;
                if (interval > 1) return Math.floor(interval) + " anos atrás";
                interval = seconds / 2592000;
                if (interval > 1) return Math.floor(interval) + " meses atrás";
                interval = seconds / 86400;
                if (interval > 1) return Math.floor(interval) + " dias atrás";
                interval = seconds / 3600;
                if (interval > 1) return Math.floor(interval) + " horas atrás";
                interval = seconds / 60;
                if (interval > 1) return Math.floor(interval) + " minutos atrás";
                return "Agora mesmo";
            },

            showDashboardView(viewName) {
                const dashEls = App.elements.dashboard;
                // Hide all views first
                dashEls.selector.style.display = 'none';
                dashEls.brocaView.style.display = 'none';
                dashEls.perdaView.style.display = 'none';
                dashEls.aereaView.style.display = 'none';
                dashEls.plantioView.style.display = 'none';
                dashEls.cigarrinhaView.style.display = 'none';
                dashEls.climaView.style.display = 'none';

                App.charts.destroyAll();

                switch (viewName) {
                    case 'selector':
                        dashEls.selector.style.display = 'grid';
                        break;
                    case 'broca':
                        dashEls.brocaView.style.display = 'block';
                        this.loadDashboardDates('broca');
                        setTimeout(() => App.charts.renderBrocaDashboardCharts(), 150);
                        break;
                    case 'perda':
                        dashEls.perdaView.style.display = 'block';
                        this.loadDashboardDates('perda');
                        setTimeout(() => App.charts.renderPerdaDashboardCharts(), 150);
                        break;
                    case 'aerea':
                        dashEls.aereaView.style.display = 'block';
                        this.loadDashboardDates('aereo');
                        setTimeout(() => App.charts.renderAereoDashboardCharts(), 150);
                        break;
                    case 'plantio':
                        dashEls.plantioView.style.display = 'block';
                        this.loadDashboardDates('plantio');
                        setTimeout(() => App.charts.renderPlantioDashboardCharts(), 150);
                        break;
                    case 'cigarrinha':
                        dashEls.cigarrinhaView.style.display = 'block';
                        this.loadDashboardDates('cigarrinha');
                        setTimeout(() => App.charts.renderCigarrinhaDashboardCharts(), 150);
                        break;
                    case 'clima':
                        dashEls.climaView.style.display = 'block';
                        this.loadDashboardDates('clima');
                        setTimeout(() => App.charts.renderClimaDashboardCharts(), 150);
                        break;
                }
            },
            setDefaultDatesForEntryForms() {
                const today = new Date().toISOString().split('T')[0];
                App.elements.broca.data.value = today;
                App.elements.perda.data.value = today;
                App.elements.cigarrinha.data.value = today;
                App.elements.cigarrinhaAmostragem.data.value = today;
                App.elements.apontamentoPlantio.date.value = today;
                if (App.elements.lancamentoClima && App.elements.lancamentoClima.data) App.elements.lancamentoClima.data.value = today;
                App.elements.broca.data.max = today;
                App.elements.perda.data.max = today;
                App.elements.cigarrinha.data.max = today;
                App.elements.cigarrinhaAmostragem.data.max = today;
                if (App.elements.lancamentoClima && App.elements.lancamentoClima.data) App.elements.lancamentoClima.data.max = today;
            },
            setDefaultDatesForReportForms() {
                const today = new Date();
                const firstDayOfMonth = new Date(today.getFullYear(), today.getMonth(), 1).toISOString().split('T')[0];
                const todayDate = today.toISOString().split('T')[0];

                const reportSections = ['broca', 'perda', 'cigarrinha', 'cigarrinhaAmostragem', 'relatorioMonitoramento', 'relatorioClima'];

                reportSections.forEach(section => {
                    const els = App.elements[section];
                    if (els) {
                        const inicioEl = els.filtroInicio || els.inicio;
                        const fimEl = els.filtroFim || els.fim;

                        if (inicioEl) {
                            inicioEl.value = firstDayOfMonth;
                        }
                        if (fimEl) {
                            fimEl.value = todayDate;
                        }
                    }
                });
            },
            setDefaultDatesForDashboard(type) {
                const today = new Date();
                const firstDayOfYear = new Date(today.getFullYear(), 0, 1).toISOString().split('T')[0];
                const todayDate = today.toISOString().split('T')[0];

                if (type === 'broca') {
                    App.elements.dashboard.brocaDashboardInicio.value = firstDayOfYear;
                    App.elements.dashboard.brocaDashboardFim.value = todayDate;
                } else if (type === 'perda') {
                    App.elements.dashboard.perdaDashboardInicio.value = firstDayOfYear;
                    App.elements.dashboard.perdaDashboardFim.value = todayDate;
                } else if (type === 'clima') {
                    document.getElementById('climaDashboardInicio').value = firstDayOfYear;
                    document.getElementById('climaDashboardFim').value = todayDate;
                }
                App.actions.saveDashboardDates(type, firstDayOfYear, todayDate);
            },
            loadDashboardDates(type) {
                const savedDates = App.actions.getDashboardDates(type);
                if (savedDates.start && savedDates.end) {
                    if (type === 'broca') {
                        App.elements.dashboard.brocaDashboardInicio.value = savedDates.start;
                        App.elements.dashboard.brocaDashboardFim.value = savedDates.end;
                    } else if (type === 'perda') {
                        App.elements.dashboard.perdaDashboardInicio.value = savedDates.start;
                        App.elements.dashboard.perdaDashboardFim.value = savedDates.end;
                    } else if (type === 'clima') {
                        document.getElementById('climaDashboardInicio').value = savedDates.start;
                        document.getElementById('climaDashboardFim').value = savedDates.end;
                    }
                } else {
                    this.setDefaultDatesForDashboard(type);
                }
            },
            clearForm(formElement) {
                if (!formElement) return;
                const inputs = formElement.querySelectorAll('input, select, textarea');
                inputs.forEach(input => {
                    if (input.type === 'checkbox' || input.type === 'radio') {
                        input.checked = false;
                    } else if (input.type !== 'date') {
                        input.value = '';
                    }
                });
                formElement.querySelectorAll('.info-display').forEach(el => el.textContent = '');
                formElement.querySelectorAll('.resultado').forEach(el => el.textContent = '');
            },
            populateFazendaSelects() {
                const selects = [
                    App.elements.broca.filtroFazenda,
                    App.elements.perda.filtroFazenda,
                    App.elements.planejamento.fazenda,
                    App.elements.harvest.fazenda,
                    App.elements.cadastros.farmSelect,
                    App.elements.broca.codigo,
                    App.elements.perda.codigo,
                    App.elements.cigarrinha.codigo,
                    App.elements.cigarrinhaAmostragem.codigo,
                    App.elements.cigarrinha.filtroFazenda,
                    App.elements.cigarrinhaAmostragem.filtroFazenda,
                    App.elements.relatorioMonitoramento.fazendaFiltro,
                    App.elements.apontamentoPlantio.farmName,
                    App.elements.lancamentoClima.fazenda,
                    App.elements.relatorioClima.fazenda,
                    document.getElementById('climaDashboardFazenda')
                ];

                const unavailableTalhaoIds = App.actions.getUnavailableTalhaoIds();

                selects.forEach(select => {
                    if (!select) return;
                    const currentValue = select.value;
                    let firstOption = '<option value="">Selecione...</option>';
                    if (select.id.includes('Filtro')) {
                        firstOption = '<option value="">Todas</option>';
                    }
                    select.innerHTML = firstOption;

                    let farmsToShow = App.state.fazendas;

                    if (select.id === 'harvestFazenda') {
                        const editingGroupId = App.elements.harvest.editingGroupId.value;
                        let farmOfEditedGroup = null;

                        if (editingGroupId && App.state.activeHarvestPlan) {
                            const editedGroup = App.state.activeHarvestPlan.sequence.find(g => g.id == editingGroupId);
                            if (editedGroup) {
                                farmOfEditedGroup = App.state.fazendas.find(f => f.code === editedGroup.fazendaCodigo);
                            }
                        }

                        farmsToShow = App.state.fazendas.filter(farm => {
                            if (farmOfEditedGroup && farm.id === farmOfEditedGroup.id) {
                                return true; // Always show the farm being edited.
                            }
                            if (!farm.talhoes || farm.talhoes.length === 0) {
                                return false;
                            }
                            const hasAvailablePlot = farm.talhoes.some(talhao => !unavailableTalhaoIds.has(talhao.id));
                            return hasAvailablePlot;
                        });
                    }

                    farmsToShow.sort((a, b) => parseInt(a.code) - parseInt(b.code)).forEach(farm => {
                        select.innerHTML += `<option value="${farm.id}">${farm.code} - ${farm.name}</option>`;
                    });

                    select.value = currentValue;
                });
            },
            populateUserSelects(selects) {
                if (!selects || selects.length === 0) return;

                selects.forEach(select => {
                    if (!select) return;
                    const currentValue = select.value;
                    select.innerHTML = '<option value="">Selecione um utilizador...</option>';
                    App.state.users
                        .filter(u => u.active)
                        .sort((a, b) => (a.username || '').localeCompare(b.username || ''))
                        .forEach(user => {
                            select.innerHTML += `<option value="${user.id}">${user.username || user.email}</option>`;
                        });
                    select.value = currentValue;
                });
            },
            populateOperatorSelects() {
                const selects = [App.elements.perda.filtroOperador];
                selects.forEach(select => {
                    if (!select) return;

                    const currentValue = select.value;
                    let firstOptionHTML = '';
                    if (select.id === 'operadorFiltroPerda') {
                        firstOptionHTML = '<option value="">Todos</option>';
                    } else {
                        firstOptionHTML = '<option value="">Selecione um operador...</option>';
                    }
                    select.innerHTML = firstOptionHTML;
                    
                    App.state.personnel
                        .sort((a, b) => a.name.localeCompare(b.name))
                        .forEach(p => {
                            select.innerHTML += `<option value="${p.matricula}">${p.matricula} - ${p.name}</option>`;
                        });
                    select.value = currentValue;
                });
            },
            renderFarmSelect() {
                const { farmSelect } = App.elements.cadastros;
                const currentValue = farmSelect.value;
                farmSelect.innerHTML = '<option value="">Selecione uma fazenda para gerir...</option>';
                App.state.fazendas.sort((a,b) => parseInt(a.code) - parseInt(b.code)).forEach(farm => {
                    farmSelect.innerHTML += `<option value="${farm.id}">${farm.code} - ${farm.name}</option>`;
                });
                farmSelect.value = currentValue;
                if(!currentValue) {
                    App.elements.cadastros.talhaoManagementContainer.style.display = 'none';
                }
            },
            renderTalhaoList(farmId) {
                const { talhaoList, talhaoManagementContainer, selectedFarmName, selectedFarmTypes } = App.elements.cadastros;
                const farm = App.state.fazendas.find(f => f.id === farmId);
                talhaoList.innerHTML = '';
                if (!farm) {
                    talhaoManagementContainer.style.display = 'none';
                    selectedFarmName.innerHTML = '';
                    selectedFarmTypes.innerHTML = '';
                    return;
                }
                talhaoManagementContainer.style.display = 'block';
                
                selectedFarmName.innerHTML = `${farm.code} - ${farm.name}`;
                
                const farmTypesHTML = farm.types && farm.types.length > 0 ? `(${farm.types.join(', ')})` : '';
                selectedFarmTypes.innerHTML = `
                    <span style="font-weight: 500; font-size: 14px; color: var(--color-text-light); margin-left: 10px;">
                        ${farmTypesHTML}
                    </span>
                    <div style="display: inline-flex; gap: 5px; margin-left: 10px;">
                        <button class="btn-excluir" style="background:var(--color-info); margin-left: 0;" data-action="edit-farm" data-id="${farm.id}"><i class="fas fa-edit"></i></button>
                        <button class="btn-excluir" data-action="delete-farm" data-id="${farm.id}"><i class="fas fa-trash"></i></button>
                    </div>
                `;

                if (!farm.talhoes || farm.talhoes.length === 0) {
                    talhaoList.innerHTML = '<p>Nenhum talhão cadastrado para esta fazenda.</p>';
                    return;
                }
                const table = document.createElement('table');
                table.id = 'personnelTable';
                table.className = 'harvestPlanTable';
                table.innerHTML = `<thead><tr><th>Nome</th><th>Área</th><th>TCH</th><th>Produção</th><th>Variedade</th><th>Corte</th><th>Distância</th><th>Última Colheita</th><th>Ações</th></tr></thead><tbody></tbody>`;
                const tbody = table.querySelector('tbody');
                farm.talhoes.sort((a,b) => a.name.localeCompare(b.name)).forEach(talhao => {
                    const row = tbody.insertRow();
                    const dataColheita = App.actions.formatDateForDisplay(talhao.dataUltimaColheita);

                    row.innerHTML = `
                        <td data-label="Nome">${talhao.name}</td>
                        <td data-label="Área">${talhao.area ? talhao.area.toFixed(2) : ''}</td>
                        <td data-label="TCH">${talhao.tch ? talhao.tch.toFixed(2) : ''}</td>
                        <td data-label="Produção">${talhao.producao ? talhao.producao.toFixed(2) : ''}</td>
                        <td data-label="Variedade">${talhao.variedade || ''}</td>
                        <td data-label="Corte">${talhao.corte || ''}</td>
                        <td data-label="Distância">${talhao.distancia ? talhao.distancia.toFixed(2) : ''}</td>
                        <td data-label="Última Colheita">${dataColheita}</td>
                        <td data-label="Ações">
                            <div style="display: flex; justify-content: flex-end; gap: 5px;">
                                <button class="btn-excluir" style="background:var(--color-info)" data-action="edit-talhao" data-id="${talhao.id}"><i class="fas fa-edit"></i></button>
                                <button class="btn-excluir" data-action="delete-talhao" data-id="${talhao.id}"><i class="fas fa-trash"></i></button>
                            </div>
                        </td>
                    `;
                });
                talhaoList.appendChild(table);
            },
            renderHarvestTalhaoSelection(farmId, plotIdsToCheck = []) {
                const { talhaoSelectionList, editingGroupId, selectAllTalhoes } = App.elements.harvest;
                talhaoSelectionList.innerHTML = '';
                selectAllTalhoes.checked = false;
                
                if (!farmId) {
                    talhaoSelectionList.innerHTML = '<p style="grid-column: 1 / -1; text-align: center;">Selecione uma fazenda para ver os talhões.</p>';
                    return;
                }
                
                const farm = App.state.fazendas.find(f => f.id === farmId);
                if (!farm || !farm.talhoes || farm.talhoes.length === 0) {
                    talhaoSelectionList.innerHTML = '<p style="grid-column: 1 / -1; text-align: center;">Nenhum talhão cadastrado nesta fazenda.</p>';
                    return;
                }
                
                const allUnavailableTalhaoIds = App.actions.getUnavailableTalhaoIds({ editingGroupId: editingGroupId.value });
                const closedTalhaoIds = new Set(App.state.activeHarvestPlan?.closedTalhaoIds || []);
                
                const availableTalhoes = farm.talhoes.filter(t => !allUnavailableTalhaoIds.has(t.id));
        
                const talhoesToShow = [...availableTalhoes];
                if (plotIdsToCheck.length > 0) {
                    const currentlyEditedTalhoes = farm.talhoes.filter(t => plotIdsToCheck.includes(t.id));
                    currentlyEditedTalhoes.forEach(t => {
                        if (!talhoesToShow.some(ts => ts.id === t.id)) {
                            talhoesToShow.push(t);
                        }
                    });
                }
        
                if (talhoesToShow.length === 0) {
                    talhaoSelectionList.innerHTML = '<p style="grid-column: 1 / -1; text-align: center;">Todos os talhões desta fazenda já foram alocados ou encerrados.</p>';
                    return;
                }
        
                talhoesToShow.sort((a,b) => a.name.localeCompare(b.name)).forEach(talhao => {
                    const isChecked = plotIdsToCheck.includes(talhao.id);
                    const isClosed = closedTalhaoIds.has(talhao.id);
                    
                    const label = document.createElement('label');
                    label.className = 'talhao-selection-item';
                    if (isClosed) {
                        label.classList.add('talhao-closed');
                    }
                    label.htmlFor = `talhao-select-${talhao.id}`;
            
                    label.innerHTML = `
                        <input type="checkbox" id="talhao-select-${talhao.id}" data-talhao-id="${talhao.id}" ${isChecked ? 'checked' : ''} ${isClosed ? 'disabled' : ''}>
                        <div class="talhao-name">${talhao.name}</div>
                        <div class="talhao-details">
                            <span><i class="fas fa-ruler-combined"></i>Área: ${talhao.area ? talhao.area.toFixed(2) : 0} ha</span>
                            <span><i class="fas fa-weight-hanging"></i>Produção: ${talhao.producao ? talhao.producao.toFixed(2) : 0} ton</span>
                            <span><i class="fas fa-seedling"></i>Variedade: ${talhao.variedade || 'N/A'}</span>
                            <span><i class="fas fa-cut"></i>Corte: ${talhao.corte || 'N/A'}</span>
                        </div>
                        ${isClosed ? '<div class="talhao-closed-overlay">Encerrado</div>' : ''}
                    `;
                    talhaoSelectionList.appendChild(label);
                });
            },
            updatePermissionsForRole(role, containerSelector = '#gerenciarUsuarios .permission-grid') {
                const permissions = App.config.roles[role] || {};
                const container = document.querySelector(containerSelector);
                if (container) {
                    container.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                        const key = cb.dataset.permission;
                        cb.checked = !!permissions[key];
                    });
                }
            },
            renderCompaniesList() {
                const { list } = App.elements.companyManagement;
                list.innerHTML = '';
                if (App.state.companies.length === 0) {
                    list.innerHTML = '<p>Nenhuma empresa cadastrada.</p>';
                    return;
                }
                const table = document.createElement('table');
                table.id = 'companiesTable';
                table.className = 'harvestPlanTable'; // Reutilizando estilo
                table.innerHTML = `<thead><tr><th>Nome da Empresa</th><th>Status</th><th>Data de Criação</th><th>Ações</th></tr></thead><tbody></tbody>`;
                const tbody = table.querySelector('tbody');

                App.state.companies.sort((a,b) => a.name.localeCompare(b.name)).forEach(c => {
                    const row = tbody.insertRow();
                    const creationDate = c.createdAt?.toDate ? c.createdAt.toDate().toLocaleDateString('pt-BR') : 'N/A';

                    // Define o status e o estilo do botão
                    const isActive = c.active !== false; // Considera ativo se 'active' for true ou undefined
                    const statusText = isActive ? 'Ativa' : 'Inativa';
                    const statusClass = isActive ? 'status-active' : 'status-inactive';
                    const buttonText = isActive ? 'Desativar' : 'Ativar';
                    const buttonClass = isActive ? 'btn-excluir' : 'btn-ativar'; // Usa 'btn-excluir' para vermelho, 'btn-ativar' para verde
                    const buttonIcon = isActive ? 'fa-ban' : 'fa-check-circle';

                    row.innerHTML = `
                        <td data-label="Nome">${c.name}</td>
                        <td data-label="Status"><span class="status-badge ${statusClass}">${statusText}</span></td>
                        <td data-label="Data de Criação">${creationDate}</td>
                        <td data-label="Ações">
                            <div style="display: flex; justify-content: flex-end; gap: 5px;">
                                <button class="btn-excluir" style="background:var(--color-purple);" data-action="view-as-company" data-id="${c.id}" title="Ver como ${c.name}"><i class="fas fa-eye"></i></button>
                                <button class="btn-excluir" style="background:var(--color-info);" data-action="edit-company" data-id="${c.id}" title="Editar Módulos"><i class="fas fa-edit"></i></button>
                                <button class="${buttonClass}" data-action="toggle-company" data-id="${c.id}" title="${buttonText} Empresa"><i class="fas ${buttonIcon}"></i></button>
                                <button class="btn-excluir-permanente" data-action="delete-company-permanently" data-id="${c.id}" title="Excluir Permanentemente"><i class="fas fa-skull-crossbones"></i></button>
                            </div>
                        </td>
                    `;
                });
                list.appendChild(table);
            },
            _createModernUserCardHTML(user) {
                const getRoleInfo = (role) => {
                    const roles = { 
                        "super-admin": ['Super Admin', 'var(--color-ai)'],
                        admin: ['Administrador', 'var(--color-danger)'], 
                        supervisor: ['Supervisor', 'var(--color-warning)'], 
                        tecnico: ['Técnico', 'var(--color-info)'], 
                        colaborador: ['Colaborador', 'var(--color-purple)'], 
                        user: ['Utilizador', 'var(--color-text-light)'] 
                    };
                    return roles[role] || ['Desconhecido', '#718096'];
                };
        
                const [roleName, roleColor] = getRoleInfo(user.role);
                const avatarLetter = (user.username || user.email).charAt(0).toUpperCase();

                const company = App.state.companies.find(c => c.id === user.companyId);
                const companyName = company ? company.name : null;
                const companyHTML = companyName ? `<span class="user-card-role" style="background-color: var(--color-text-light); margin-left: 8px;"><i class="fas fa-building"></i> ${companyName}</span>` : '';
        
                const buttonsHTML = user.email.toLowerCase() === 'admin@agrovetor.com' ? '' : `
                    <button class="toggle-btn ${user.active ? 'inactive' : 'active'}" data-action="toggle" data-id="${user.id}">
                        ${user.active ? '<i class="fas fa-ban"></i> Desativar' : '<i class="fas fa-check"></i> Ativar'}
                    </button>
                    <button data-action="edit" data-id="${user.id}"><i class="fas fa-edit"></i> Editar</button>
                `;
        
                return `
                    <div class="user-card-redesigned" style="border-left-color: ${roleColor};">
                        <div class="user-card-header">
                            <div class="user-card-info">
                                <div class="user-card-avatar" style="background-color: ${roleColor}20; color: ${roleColor};">${avatarLetter}</div>
                                <div class="user-card-details">
                                    <h4>${user.username || 'N/A'}</h4>
                                    <p>${user.email}</p>
                                </div>
                            </div>
                            <div class="user-card-status ${user.active ? 'active' : 'inactive'}">
                                <i class="fas fa-circle"></i> ${user.active ? 'Ativo' : 'Inativo'}
                            </div>
                        </div>
                        <div>
                            <span class="user-card-role" style="background-color: ${roleColor};">${roleName}</span>
                            ${companyHTML}
                        </div>
                        <div class="user-card-actions">
                            ${buttonsHTML}
                        </div>
                    </div>`;
            },
            async renderSyncHistory() {
                const listEl = document.getElementById('syncHistoryList');
                if (!listEl) return;

                listEl.innerHTML = '<div class="spinner-container" style="display:flex; justify-content:center; padding: 20px;"><div class="spinner"></div></div>';

                try {
                    // Consulta o Firestore para obter o histórico da empresa atual
                    const q = query(
                        collection(db, 'sync_history_store'),
                        where("companyId", "==", App.state.currentUser.companyId),
                        orderBy("timestamp", "desc")
                    );
                    const querySnapshot = await getDocs(q);

                    listEl.innerHTML = '';

                    if (querySnapshot.empty) {
                        listEl.innerHTML = '<p style="text-align:center; padding: 20px; color: var(--color-text-light);">Nenhum histórico de sincronização encontrado.</p>';
                        return;
                    }

                    const statusMap = {
                        success: { icon: 'fa-check-circle', color: 'var(--color-success)', label: 'Sucesso' },
                        partial: { icon: 'fa-exclamation-triangle', color: 'var(--color-warning)', label: 'Parcial' },
                        failure: { icon: 'fa-exclamation-circle', color: 'var(--color-danger)', label: 'Falha' },
                        no_data: { icon: 'fa-info-circle', color: 'var(--color-info)', label: 'Informativo' },
                        critical_error: { icon: 'fa-bomb', color: 'var(--color-danger)', label: 'Erro Crítico' },
                    };

                    querySnapshot.forEach(doc => {
                        const log = doc.data();
                        const logId = doc.id;
                        const logTimestamp = log.timestamp ? log.timestamp.toDate() : new Date(); // Lida com timestamps pendentes

                        const statusInfo = statusMap[log.status] || { icon: 'fa-question-circle', color: 'var(--color-text-light)', label: 'Desconhecido' };
                        const card = document.createElement('div');
                        card.className = 'plano-card';
                        card.style.borderLeftColor = statusInfo.color;

                        const detailsButton = (log.items && log.items.length > 0)
                            ? `<button class="btn-excluir" style="background-color: var(--color-info); margin-left: 0;" data-action="view-sync-details" data-id="${logId}">
                                   <i class="fas fa-eye"></i> Ver Detalhes
                               </button>`
                            : '';

                        card.innerHTML = `
                            <div class="plano-header">
                                <span class="plano-title"><i class="fas ${statusInfo.icon}" style="color: ${statusInfo.color};"></i> Sincronização por ${log.username || 'Sistema'}</span>
                                <span class="plano-status" style="background-color: ${statusInfo.color}; font-size: 12px; text-transform: none;">
                                    ${logTimestamp.toLocaleString('pt-BR')}
                                </span>
                            </div>
                            <div class="plano-details" style="grid-template-columns: 1fr;">
                                <div><i class="fas fa-comment-alt"></i> Detalhes: ${log.details}</div>
                            </div>
                            <div class="plano-actions">
                                ${detailsButton}
                            </div>
                        `;
                        listEl.appendChild(card);
                    });

                } catch (error) {
                    console.error("Erro ao renderizar histórico de sincronização do Firestore:", error);
                    listEl.innerHTML = '<p style="text-align:center; padding: 20px; color: var(--color-danger);">Erro ao carregar o histórico.</p>';
                }
            },
            renderUsersList() { 
                const { list } = App.elements.users; 
                list.innerHTML = App.state.users
                    .sort((a,b) => (a.username || '').localeCompare(b.username || ''))
                    .map((u) => this._createModernUserCardHTML(u))
                    .join(''); 
            },
            renderFrenteDePlantioList() {
                const { list } = App.elements.frenteDePlantio;
                list.innerHTML = '';
                if (App.state.frentesDePlantio.length === 0) {
                    list.innerHTML = '<p>Nenhuma frente de plantio cadastrada.</p>';
                    return;
                }
                const table = document.createElement('table');
                table.id = 'frenteDePlantioTable';
                table.className = 'harvestPlanTable';
                table.innerHTML = `<thead><tr><th>Nome</th><th>Prestador</th><th>Observação</th><th>Ações</th></tr></thead><tbody></tbody>`;
                const tbody = table.querySelector('tbody');
                App.state.frentesDePlantio.sort((a,b) => a.name.localeCompare(b.name)).forEach(f => {
                    const row = tbody.insertRow();
                    row.innerHTML = `
                        <td data-label="Nome">${f.name}</td>
                        <td data-label="Prestador">${f.provider}</td>
                        <td data-label="Observação">${f.obs || ''}</td>
                        <td data-label="Ações">
                            <div style="display: flex; justify-content: flex-end; gap: 5px;">
                                <button class="btn-excluir" style="background:var(--color-info)" data-action="edit-frente" data-id="${f.id}"><i class="fas fa-edit"></i></button>
                                <button class="btn-excluir" data-action="delete-frente" data-id="${f.id}"><i class="fas fa-trash"></i></button>
                            </div>
                        </td>
                    `;
                });
                list.appendChild(table);
            },

            populateFrenteDePlantioSelect() {
                const selects = [App.elements.apontamentoPlantio.frente, App.elements.relatorioPlantio.frente];
                selects.forEach(select => {
                    if (!select) return;
                    const currentValue = select.value;
                    let firstOption = '<option value="">Selecione...</option>';
                    if (select.id === 'plantioRelatorioFrente') {
                        firstOption = '<option value="">Todas</option>';
                    }
                    select.innerHTML = firstOption;
                    App.state.frentesDePlantio.sort((a, b) => a.name.localeCompare(b.name)).forEach(f => {
                        select.innerHTML += `<option value="${f.id}">${f.name}</option>`;
                    });
                    select.value = currentValue;
                });
            },

            addPlantioRecordCard() {
                const container = App.elements.apontamentoPlantio.recordsContainer;
                if (!container) return;

                container.querySelectorAll('.amostra-card:not(.collapsed)').forEach(c => c.classList.add('collapsed'));

                const recordId = Date.now();
                const card = document.createElement('div');
                card.className = 'amostra-card';
                card.dataset.id = recordId;

                const recordCount = container.children.length + 1;

                card.innerHTML = `
                    <div class="amostra-header" style="cursor: pointer;">
                        <i class="fas fa-chevron-down amostra-toggle-icon"></i>
                        <h4>Lançamento ${recordCount}</h4>
                        <button type="button" class="btn-remover-amostra" title="Remover Lançamento">&times;</button>
                    </div>
                    <div class="amostra-body">
                        <div class="form-row">
                            <div class="form-col">
                                <label for="plantioTalhao-${recordId}" class="required">Talhão:</label>
                                <select id="plantioTalhao-${recordId}" class="plantio-record-input plantio-talhao-select" required></select>
                                <div id="plantioTalhaoInfo-${recordId}" class="info-display"></div>
                            </div>
                            <div class="form-col">
                                <label for="plantioVariedade-${recordId}" class="required">Variedade Plantada:</label>
                                <input type="text" id="plantioVariedade-${recordId}" class="plantio-record-input" required style="text-transform:uppercase" oninput="this.value = this.value.toUpperCase()">
                            </div>
                            <div class="form-col">
                                <label for="plantioArea-${recordId}" class="required">Área Plantada (ha):</label>
                                <input type="number" id="plantioArea-${recordId}" class="plantio-record-input plantio-area-input" required>
                            </div>
                        </div>
                    </div>
                `;
                container.appendChild(card);
                card.querySelector('select').focus();
                this.calculateTotalPlantedArea();
                this.populateTalhaoSelect(card);

                const talhaoSelect = card.querySelector('.plantio-talhao-select');
                talhaoSelect.addEventListener('change', () => this.updateTalhaoInfo(card));
            },

            populateTalhaoSelect(card) {
                const farmId = App.elements.apontamentoPlantio.farmName.value;
                const talhaoSelect = card.querySelector('.plantio-talhao-select');
                talhaoSelect.innerHTML = '<option value="">Selecione...</option>';
                if (farmId) {
                    const farm = App.state.fazendas.find(f => f.id === farmId);
                    if (farm && farm.talhoes) {
                        farm.talhoes.forEach(talhao => {
                            talhaoSelect.innerHTML += `<option value="${talhao.id}">${talhao.name}</option>`;
                        });
                    }
                }
            },

            updateAllTalhaoSelects() {
                const recordCards = App.elements.apontamentoPlantio.recordsContainer.querySelectorAll('.amostra-card');
                recordCards.forEach(card => {
                    this.populateTalhaoSelect(card);
                    this.updateTalhaoInfo(card);
                });
            },

            async updateTalhaoInfo(card) {
                const talhaoId = card.querySelector('.plantio-talhao-select').value;
                const infoDiv = card.querySelector('.info-display');
                const editingEntryId = App.elements.apontamentoPlantio.entryId.value; // Get ID if we are editing

                if (!talhaoId) {
                    infoDiv.textContent = '';
                    return;
                }

                const farmId = App.elements.apontamentoPlantio.farmName.value;
                const farm = App.state.fazendas.find(f => f.id === farmId);
                if (!farm) { // Defensive check
                    infoDiv.textContent = 'Fazenda não encontrada.';
                    return;
                }
                const talhao = farm.talhoes.find(t => t.id == talhaoId);
                if (!talhao) { // Defensive check
                    infoDiv.textContent = 'Talhão não encontrado.';
                    return;
                }


                let plantedAreaByOthers = 0;
                App.state.apontamentosPlantio.forEach(apontamento => {
                    // If we are editing, and this is the entry we are currently editing, skip its records from the sum.
                    if (editingEntryId && apontamento.id === editingEntryId) {
                        return;
                    }
                    apontamento.records.forEach(record => {
                        if (record.talhaoId === talhaoId) {
                            plantedAreaByOthers += record.area;
                        }
                    });
                });

                const remainingArea = talhao.area - plantedAreaByOthers;
                infoDiv.textContent = `Área: ${talhao.area.toFixed(2)}ha | Plantado (outros): ${plantedAreaByOthers.toFixed(2)}ha | Restante: ${remainingArea.toFixed(2)}ha`;
                card.querySelector('.plantio-area-input').max = remainingArea;
            },

            calculateTotalPlantedArea() {
                const container = App.elements.apontamentoPlantio.recordsContainer;
                const totalAreaEl = App.elements.apontamentoPlantio.totalArea;
                if (!container || !totalAreaEl) return;

                let totalArea = 0;
                container.querySelectorAll('.plantio-area-input').forEach(input => {
                    totalArea += parseFloat(input.value) || 0;
                });

                totalAreaEl.textContent = `Total de Área Plantada: ${totalArea.toFixed(2).replace('.', ',')} ha`;
            },

            renderPersonnelList() {
                const { list } = App.elements.personnel;
                list.innerHTML = '';
                if (App.state.personnel.length === 0) {
                    list.innerHTML = '<p>Nenhuma pessoa cadastrada.</p>';
                    return;
                }
                const table = document.createElement('table');
                table.id = 'personnelTable';
                table.className = 'harvestPlanTable';
                table.innerHTML = `<thead><tr><th>Matrícula</th><th>Nome</th><th>Ações</th></tr></thead><tbody></tbody>`;
                const tbody = table.querySelector('tbody');
                App.state.personnel.sort((a,b) => a.name.localeCompare(b.name)).forEach(p => {
                    const row = tbody.insertRow();
                    row.innerHTML = `
                        <td data-label="Matrícula">${p.matricula}</td>
                        <td data-label="Nome">${p.name}</td>
                        <td data-label="Ações">
                            <div style="display: flex; justify-content: flex-end; gap: 5px;">
                                <button class="btn-excluir" style="background:var(--color-info)" data-action="edit-personnel" data-id="${p.id}"><i class="fas fa-edit"></i></button>
                                <button class="btn-excluir" data-action="delete-personnel" data-id="${p.id}"><i class="fas fa-trash"></i></button>
                            </div>
                        </td>
                    `;
                });
                list.appendChild(table);
            },
            renderLogoPreview() {
                const { logoPreview, removeLogoBtn } = App.elements.companyConfig;
                if (App.state.companyLogo) {
                    logoPreview.src = App.state.companyLogo;
                    logoPreview.style.display = 'block';
                    removeLogoBtn.style.display = 'inline-flex';
                } else {
                    logoPreview.style.display = 'none';
                    removeLogoBtn.style.display = 'none';
                }
            },
            renderGerenciamento() {
                const { lista, dataType, startDate, endDate } = App.elements.gerenciamento;
                lista.innerHTML = '';
                let content = '';

                const type = dataType.value;
                const start = startDate.value;
                const end = endDate.value;

                if (type === 'apontamentoPlantio') {
                    let apontamentosFiltrados = App.state.apontamentosPlantio;
                    if (start) {
                        apontamentosFiltrados = apontamentosFiltrados.filter(a => a.date >= start);
                    }
                    if (end) {
                        apontamentosFiltrados = apontamentosFiltrados.filter(a => a.date <= end);
                    }
                    if (apontamentosFiltrados.length > 0) {
                        content += `<h3>Apontamento de Plantio (${apontamentosFiltrados.length})</h3>`;
                        content += apontamentosFiltrados.map((ap) => `<div class="user-card"><strong>${ap.farmName}</strong> - ${ap.date} <button class="btn-excluir" data-action="delete" data-type="apontamentoPlantio" data-id="${ap.id}"><i class="fas fa-trash"></i> Excluir</button><button class="btn-excluir" style="background-color: var(--color-info);" data-action="edit" data-type="apontamentoPlantio" data-id="${ap.id}"><i class="fas fa-edit"></i> Editar</button></div>`).join('');
                    }
                } else {
                    let registrosFiltrados = App.state.registros;
                    let perdasFiltradas = App.state.perdas;

                    if (start) {
                        registrosFiltrados = registrosFiltrados.filter(r => r.data >= start);
                        perdasFiltradas = perdasFiltradas.filter(p => p.data >= start);
                    }
                    if (end) {
                        registrosFiltrados = registrosFiltrados.filter(r => r.data <= end);
                        perdasFiltradas = perdasFiltradas.filter(p => p.data <= end);
                    }

                    if (type === 'brocamento') {
                        if (registrosFiltrados.length > 0) {
                            content += `<h3>Brocamento (${registrosFiltrados.length})</h3>`;
                            content += registrosFiltrados.map((reg) => `<div class="user-card"><strong>${reg.fazenda}</strong> - ${reg.talhao} (${reg.data}) <button class="btn-excluir" data-action="delete" data-type="brocamento" data-id="${reg.id}"><i class="fas fa-trash"></i> Excluir</button></div>`).join('');
                        }
                    }
                    if (type === 'perda') {
                        if (perdasFiltradas.length > 0) {
                            content += `<h3 style="margin-top:20px;">Perda de Cana (${perdasFiltradas.length})</h3>`;
                            content += perdasFiltradas.map((p) => `<div class="user-card"><strong>${p.fazenda}</strong> - ${p.talhao} (${p.data}) <button class="btn-excluir" data-action="delete" data-type="perda" data-id="${p.id}"><i class="fas fa-trash"></i> Excluir</button></div>`).join('');
                        }
                    }
                }

                lista.innerHTML = content || '<p style="text-align:center; padding: 20px;">Nenhum lançamento encontrado para os filtros selecionados.</p>';
            },
            renderPlanejamento() {
                const { lista } = App.elements.planejamento; lista.innerHTML = '';
                const hoje = new Date(); hoje.setHours(0,0,0,0);
                const planosOrdenados = [...App.state.planos].sort((a,b) => new Date(a.dataPrevista) - new Date(b.dataPrevista));
                if(planosOrdenados.length === 0) { lista.innerHTML = '<p style="text-align:center; padding: 20px; color: var(--color-text-light);">Nenhuma inspeção planejada.</p>'; return; }
                planosOrdenados.forEach(plano => {
                    let status = plano.status;
                    const dataPlano = new Date(plano.dataPrevista + 'T03:00:00Z');
                    if (plano.status === 'Pendente' && dataPlano < hoje) { status = 'Atrasado'; }
                    const fazenda = App.state.fazendas.find(f => f.code === plano.fazendaCodigo);
                    const fazendaNome = fazenda ? `${fazenda.code} - ${fazenda.name}` : 'Desconhecida';
                    const card = document.createElement('div'); card.className = 'plano-card';
                    card.innerHTML = `<div class="plano-header"><span class="plano-title"><i class="fas fa-${plano.tipo === 'broca' ? 'bug' : 'dollar-sign'}"></i> ${fazendaNome} - Talhão: ${plano.talhao}</span><span class="plano-status ${status.toLowerCase()}">${status}</span></div><div class="plano-details"><div><i class="fas fa-calendar-day"></i> Data Prevista: ${dataPlano.toLocaleDateString('pt-BR')}</div><div><i class="fas fa-user-check"></i> Responsável: ${plano.usuarioResponsavel}</div>${plano.meta ? `<div><i class="fas fa-bullseye"></i> Meta: ${plano.meta}</div>` : ''}</div>${plano.observacoes ? `<div style="margin-top:8px;font-size:14px;"><i class="fas fa-info-circle"></i> Obs: ${plano.observacoes}</div>` : ''}<div class="plano-actions">${status !== 'Concluído' ? `<button class="btn-excluir" style="background-color: var(--color-success)" data-action="concluir" data-id="${plano.id}"><i class="fas fa-check"></i> Marcar Concluído</button>` : ''}<button class="btn-excluir" data-action="excluir" data-id="${plano.id}"><i class="fas fa-trash"></i> Excluir</button></div>`;
                    lista.appendChild(card);
                });
            },
            async showHarvestPlanList() {
                const userId = App.state.currentUser?.uid;
                if (userId && App.state.activeHarvestPlan) {
                    try {
                        await App.data.deleteDocument('userDrafts', userId);
                    } catch (error) {
                        console.error("Não foi possível apagar o rascunho do Firestore:", error);
                    }
                }

                App.state.activeHarvestPlan = null;
                App.elements.harvest.plansListContainer.style.display = 'block';
                App.elements.harvest.planEditor.style.display = 'none';
                this.renderHarvestPlansList();
            },
            showHarvestPlanEditor() {
                App.elements.harvest.plansListContainer.style.display = 'none';
                App.elements.harvest.planEditor.style.display = 'block';
            },
            renderHarvestPlansList() {
                const { plansList } = App.elements.harvest;
                plansList.innerHTML = '';
                if(App.state.harvestPlans.length === 0) {
                    plansList.innerHTML = '<p style="text-align:center; padding: 20px; color: var(--color-text-light);">Nenhum plano de colheita criado. Clique em "Novo Plano" para começar.</p>';
                    return;
                }
                App.state.harvestPlans.forEach(plan => {
                    const totalProducao = plan.sequence.reduce((sum, group) => sum + group.totalProducao, 0);
                    const card = document.createElement('div');
                    card.className = 'plano-card';
                    card.innerHTML = `
                        <div class="plano-header">
                            <span class="plano-title"><i class="fas fa-stream"></i> ${plan.frontName}</span>
                            <span class="plano-status pendente">${plan.sequence.length} fazenda(s)</span>
                        </div>
                        <div class="plano-details">
                            <div><i class="fas fa-calendar-day"></i> Início: ${new Date(plan.startDate + 'T03:00:00Z').toLocaleDateString('pt-BR')}</div>
                            <div><i class="fas fa-tasks"></i> ${plan.dailyRate} ton/dia</div>
                            <div><i class="fas fa-weight-hanging"></i> Total: ${totalProducao.toFixed(2)} ton</div>
                        </div>
                        <div class="plano-actions">
                            <button class="btn-excluir" style="background-color: var(--color-info); margin-left: 0;" data-action="edit" data-id="${plan.id}"><i class="fas fa-edit"></i> Editar</button>
                            <button class="btn-excluir" data-action="delete" data-id="${plan.id}"><i class="fas fa-trash"></i> Excluir</button>
                        </div>
                    `;
                    plansList.appendChild(card);
                });
            },
            renderHarvestSequence() {
                if (!App.state.activeHarvestPlan) return;
                const { tableBody, summary } = App.elements.harvest;
                const { startDate, dailyRate, sequence, closedTalhaoIds = [] } = App.state.activeHarvestPlan;
                
                tableBody.innerHTML = '';
                let grandTotalProducao = 0;
                let grandTotalArea = 0;

                let currentDate = startDate ? new Date(startDate + 'T03:00:00Z') : new Date();
                if (isNaN(currentDate.getTime())) {
                    currentDate = new Date();
                }
                const dailyTon = parseFloat(dailyRate) > 0 ? parseFloat(dailyRate) : 1;

                sequence.forEach((group, index) => {
                    const producaoConsiderada = group.totalProducao - (group.producaoColhida || 0);

                    grandTotalProducao += group.totalProducao;
                    grandTotalArea += group.totalArea;

                    const diasNecessarios = Math.ceil(producaoConsiderada / dailyTon);
                    
                    const dataEntrada = new Date(currentDate.getTime());
                    
                    let dataSaida = new Date(dataEntrada.getTime());
                    if (diasNecessarios > 0) {
                        dataSaida.setDate(dataSaida.getDate() + diasNecessarios - 1);
                    }
                    
                    currentDate = new Date(dataSaida.getTime());
                    currentDate.setDate(currentDate.getDate() + 1);
                    
                    const idadeMediaMeses = App.actions.calculateAverageAge(group, dataEntrada);
                    const diasAplicacao = App.actions.calculateMaturadorDays(group);

                    const areaColhida = group.areaColhida || 0;
                    const producaoColhida = group.producaoColhida || 0;

                    const row = tableBody.insertRow();
                    row.draggable = true;
                    row.dataset.id = group.id;
                    
                    row.innerHTML = `
                        <td data-label="Seq.">${index + 1}</td>
                        <td data-label="Fazenda">${group.fazendaCodigo} - ${group.fazendaName}</td>
                        <td data-label="Talhões" class="talhao-list-cell">${group.plots.map(p => p.talhaoName).join(', ')}</td>
                        <td data-label="Área (ha)">${areaColhida.toFixed(2)} / ${group.totalArea.toFixed(2)}</td>
                        <td data-label="Prod. (ton)">${producaoColhida.toFixed(2)} / ${group.totalProducao.toFixed(2)}</td>
                        <td data-label="ATR"><span>${group.atr || 'N/A'}</span></td>
                        <td data-label="Idade (m)">${idadeMediaMeses}</td>
                        <td data-label="Maturador">${group.maturador || 'N/A'}</td>
                        <td data-label="Dias Aplic.">${diasAplicacao}</td>
                        <td data-label="Ação">
                            <div style="display: flex; justify-content: flex-end; gap: 5px;">
                                <button class="btn-excluir" style="background-color: var(--color-info);" title="Editar Grupo no Plano" data-action="edit-harvest-group" data-id="${group.id}"><i class="fas fa-edit"></i></button>
                                <button class="btn-excluir" title="Remover Grupo do Plano" data-action="remove-harvest" data-id="${group.id}"><i class="fas fa-times"></i></button>
                            </div>
                        </td>
                        <td data-label="Entrada">${dataEntrada.toLocaleDateString('pt-BR')}</td>
                        <td data-label="Saída">${dataSaida.toLocaleDateString('pt-BR')}</td>
                    `;
                });

                if (sequence.length > 0) {
                    const allVarieties = new Set();
                    sequence.forEach(group => {
                        const farm = App.state.fazendas.find(f => f.code === group.fazendaCodigo);
                        if(farm) {
                            group.plots.forEach(plot => {
                                const talhao = farm.talhoes.find(t => t.id === plot.talhaoId);
                                if(talhao && talhao.variedade) {
                                    allVarieties.add(talhao.variedade);
                                }
                            });
                        }
                    });
                    const varietiesString = allVarieties.size > 0 ? Array.from(allVarieties).join(', ') : 'N/A';
                    
                    const finalDate = new Date(currentDate.getTime());
                    finalDate.setDate(finalDate.getDate() - 1);

                    summary.innerHTML = `
                        <p>Produção Total (Ativa): <span>${grandTotalProducao.toFixed(2)} ton</span></p>
                        <p>Área Total (Ativa): <span>${grandTotalArea.toFixed(2)} ha</span></p>
                        <p>Data Final de Saída Prevista: <span>${finalDate.toLocaleDateString('pt-BR')}</span></p>
                        <p>Variedades na Sequência: <span>${varietiesString}</span></p>
                    `;
                } else {
                    summary.innerHTML = '<p>Adicione fazendas à sequência para ver o resumo da colheita.</p>';
                }
            },
            validateFields(ids) { return ids.every(id => { const el = document.getElementById(id); const valid = el.value.trim() !== ''; el.style.borderColor = valid ? 'var(--color-border)' : 'var(--color-danger)'; if (!valid) el.focus(); return valid; }); },
            updateBrocadoTotal() {
                const { broca } = App.elements;
                const base = parseInt(broca.base.value) || 0;
                const meio = parseInt(broca.meio.value) || 0;
                const topo = parseInt(broca.topo.value) || 0;
                broca.brocado.value = base + meio + topo;
            },
            calculateBrocamento() {
                const entrenos = parseInt(App.elements.broca.entrenos.value) || 0;
                const brocado = parseInt(App.elements.broca.brocado.value) || 0;
                const resultadoEl = App.elements.broca.resultado;
                if (entrenos > 0) {
                    const porcentagem = (brocado / entrenos) * 100;
                    resultadoEl.textContent = `Brocamento: ${porcentagem.toFixed(2).replace('.', ',')}%`;
                    resultadoEl.style.color = porcentagem > 20 ? 'var(--color-danger)' : 'var(--color-success)';
                } else {
                    resultadoEl.textContent = '';
                }
            },
            calculatePerda() {
                const fields = ['canaInteira', 'tolete', 'toco', 'ponta', 'estilhaco', 'pedaco'];
                const total = fields.reduce((sum, id) => sum + (parseFloat(document.getElementById(id).value) || 0), 0);
                App.elements.perda.resultado.textContent = `Total Perda: ${total.toFixed(2).replace('.', ',')} kg`;
            },

            calculateCigarrinha() {
                const { fase1, fase2, fase3, fase4, fase5, resultado } = App.elements.cigarrinha;
                const f1 = parseInt(fase1.value) || 0;
                const f2 = parseInt(fase2.value) || 0;
                const f3 = parseInt(fase3.value) || 0;
                const f4 = parseInt(fase4.value) || 0;
                const f5 = parseInt(fase5.value) || 0;

                // Lê o método de cálculo do estado da aplicação, com '5' como padrão.
                const divisor = parseInt(App.state.companyConfig?.cigarrinhaCalcMethod || '5', 10);

                const media = (f1 + f2 + f3 + f4 + f5) / divisor;
                resultado.textContent = `Resultado: ${media.toFixed(2).replace('.', ',')}`;
            },

            calculateCigarrinhaAmostragem() {
                const container = document.getElementById('amostrasCigarrinhaAmostragemContainer');
                const resultadoEl = document.getElementById('resultadoCigarrinhaAmostragem');
                if (!container || !resultadoEl) return;

                const amostras = container.querySelectorAll('.amostra-card');
                if (amostras.length === 0) {
                    resultadoEl.textContent = '';
                    return;
                }

                const divisor = parseInt(App.state.companyConfig?.cigarrinhaCalcMethod || '5', 10);
                let somaTotalDeFases = 0;

                amostras.forEach(card => {
                    card.querySelectorAll('.amostra-input').forEach(input => {
                        somaTotalDeFases += parseInt(input.value) || 0;
                    });
                });

                const resultadoFinal = somaTotalDeFases / divisor;

                resultadoEl.textContent = `Resultado: ${resultadoFinal.toFixed(2).replace('.', ',')}`;
            },

            showConfirmationModal(message, onConfirm, inputsConfig = false) {
                const { overlay, title, message: msgEl, confirmBtn, cancelBtn, closeBtn, inputContainer } = App.elements.confirmationModal;
                title.textContent = "Confirmar Ação";
                msgEl.textContent = message;
                
                inputContainer.innerHTML = '';
                inputContainer.style.display = 'none';

                if (inputsConfig) {
                    const inputsArray = Array.isArray(inputsConfig) ? inputsConfig : [{ id: 'confirmationModalInput', placeholder: 'Digite para confirmar' }];
                    inputContainer.style.display = 'block';

                    inputsArray.forEach(config => {
                        let inputEl;

                        if (config.type === 'select') {
                            if (config.label) {
                                const label = document.createElement('label');
                                label.htmlFor = config.id;
                                label.textContent = config.label;
                                inputContainer.appendChild(label);
                            }
                            inputEl = document.createElement('select');
                            inputEl.id = config.id;
                            if (config.options && Array.isArray(config.options)) {
                                config.options.forEach(opt => {
                                    const option = document.createElement('option');
                                    option.value = opt.value;
                                    option.textContent = opt.text;
                                    inputEl.appendChild(option);
                                });
                            }
                        } else if (config.type === 'textarea') {
                            inputEl = document.createElement('textarea');
                            inputEl.placeholder = config.placeholder || '';
                        } else {
                            inputEl = document.createElement('input');
                            inputEl.type = config.type || 'text';
                            inputEl.placeholder = config.placeholder || '';
                        }

                        inputEl.id = config.id;
                        inputEl.value = config.value || '';
                        if (config.required) {
                            inputEl.required = true;
                        }
                        inputContainer.appendChild(inputEl);
                    });

                    inputContainer.querySelector('input, textarea, select')?.focus();
                }

                const confirmHandler = () => {
                    let results = {};
                    let allValid = true;
                    if (inputsConfig) {
                        const inputs = Array.from(inputContainer.querySelectorAll('input, textarea, select'));
                        inputs.forEach(input => {
                            if (input.required && !input.value) {
                                allValid = false;
                            }
                            results[input.id] = input.value;
                        });
                    }

                    if (!allValid) {
                        App.ui.showAlert("Por favor, preencha todos os campos obrigatórios.", "error");
                        return;
                    }

                    let valueToConfirm = results;
                    // For backward compatibility with calls that expect a single string instead of a results object.
                    // This happens when `inputsConfig` is `true`, which creates a single input with a hardcoded ID.
                    if (!Array.isArray(inputsConfig) && inputsConfig === true) {
                        valueToConfirm = results['confirmationModalInput'];
                    }
                    
                    onConfirm(valueToConfirm);
                    closeHandler();
                };
                
                const closeHandler = () => {
                    overlay.classList.remove('show');
                    confirmBtn.removeEventListener('click', confirmHandler);
                    cancelBtn.removeEventListener('click', closeHandler);
                    closeBtn.removeEventListener('click', closeHandler);
                    setTimeout(() => {
                        confirmBtn.textContent = "Confirmar";
                        cancelBtn.style.display = 'inline-flex';
                    }, 300);
                };
                
                confirmBtn.addEventListener('click', confirmHandler);
                cancelBtn.addEventListener('click', closeHandler);
                closeBtn.addEventListener('click', closeHandler);
                overlay.classList.add('show');
            },
            showAdminPasswordConfirmModal() {
                App.elements.adminPasswordConfirmModal.overlay.classList.add('show');
                App.elements.adminPasswordConfirmModal.passwordInput.focus();
            },
            closeAdminPasswordConfirmModal() {
                App.elements.adminPasswordConfirmModal.overlay.classList.remove('show');
                App.elements.adminPasswordConfirmModal.passwordInput.value = '';
            },

            showEnableOfflineLoginModal() {
                const modal = document.getElementById('enableOfflineLoginModal');
                if (modal) {
                    modal.classList.add('show');
                    const passwordInput = document.getElementById('enableOfflinePassword');
                    if (passwordInput) {
                        passwordInput.value = '';
                        passwordInput.focus();
                    }
                }
            },

            closeEnableOfflineLoginModal() {
                const modal = document.getElementById('enableOfflineLoginModal');
                if (modal) {
                    modal.classList.remove('show');
                }
            },

            showImpersonationBanner(companyName) {
                this.hideImpersonationBanner(); // Limpa qualquer banner anterior

                const banner = document.createElement('div');
                banner.id = 'impersonation-banner';
                const bannerHeight = 40;

                // Estilos do banner
                Object.assign(banner.style, {
                    position: 'fixed', top: '0', left: '0', width: '100%', height: `${bannerHeight}px`,
                    backgroundColor: 'var(--color-purple)', color: 'white', textAlign: 'center',
                    display: 'flex', justifyContent: 'center', alignItems: 'center',
                    fontSize: '14px', zIndex: '10001', boxSizing: 'border-box'
                });

                // Conteúdo do banner
                banner.innerHTML = `
                    <i class="fas fa-eye" style="margin-right: 10px;"></i>
                    <span>A visualizar como <strong>${companyName}</strong>.</span>
                    <button id="stop-impersonating-btn" style="background: white; color: var(--color-purple); border: none; padding: 5px 10px; border-radius: 5px; margin-left: 20px; cursor: pointer; font-weight: bold;">Sair da Visualização</button>
                `;

                // Adiciona o banner ao corpo e ajusta o padding
                document.body.prepend(banner);
                document.body.style.paddingTop = `${bannerHeight}px`;

                // Adiciona o event listener de forma segura após o elemento estar no DOM
                const stopBtn = document.getElementById('stop-impersonating-btn');
                if (stopBtn) {
                    stopBtn.addEventListener('click', App.actions.stopImpersonating);
                }
            },

            hideImpersonationBanner() {
                const banner = document.getElementById('impersonation-banner');
                if (banner) {
                    banner.remove();
                }
                document.body.style.paddingTop = '0';
            },
            openUserEditModal(userId) {
                const modalEls = App.elements.userEditModal;
                const user = App.state.users.find(u => u.id == userId);
                if (!user) return;

                modalEls.editingUserId.value = user.id;
                modalEls.title.textContent = `Editar Utilizador: ${user.username}`;
                modalEls.username.value = user.username;
                modalEls.role.value = user.role;

                this.renderPermissionItems(modalEls.permissionGrid, user.permissions);

                modalEls.overlay.classList.add('show');
            },
            closeUserEditModal() {
                App.elements.userEditModal.overlay.classList.remove('show');
            },
            openEditCompanyModal(companyId) {
                const modal = App.elements.editCompanyModal;
                const company = App.state.companies.find(c => c.id === companyId);
                if (!company) {
                    App.ui.showAlert("Empresa não encontrada.", "error");
                    return;
                }

                modal.editingCompanyId.value = company.id;
                modal.companyNameDisplay.textContent = company.name;

                const grid = modal.modulesGrid;
                grid.innerHTML = ''; // Limpa o grid antes de preencher

                const allPermissions = App.config.menuConfig.flatMap(item =>
                    item.submenu ? item.submenu : [item]
                ).filter(item => item.permission && item.permission !== 'superAdmin');

                const subscribedModules = new Set(company.subscribedModules || []);

                allPermissions.forEach(perm => {
                    const isChecked = subscribedModules.has(perm.permission);
                    const checkboxHTML = `
                        <label class="report-option-item">
                            <input type="checkbox" data-module="${perm.permission}" ${isChecked ? 'checked' : ''}>
                            <span class="checkbox-visual"><i class="fas fa-check"></i></span>
                            <span class="option-content">
                                <i class="${perm.icon}"></i>
                                <span>${perm.label}</span>
                            </span>
                        </label>
                    `;
                    grid.innerHTML += checkboxHTML;
                });

                modal.overlay.classList.add('show');
            },
            closeEditCompanyModal() {
                App.elements.editCompanyModal.overlay.classList.remove('show');
            },
            openEditFarmModal(farmId) {
                const farm = App.state.fazendas.find(f => f.id === farmId);
                if (!farm) return;
                const modal = App.elements.editFarmModal;
                modal.editingFarmId.value = farm.id;
                modal.nameInput.value = farm.name;

                modal.typeCheckboxes.forEach(cb => {
                    cb.checked = farm.types && farm.types.includes(cb.value);
                });

                modal.overlay.classList.add('show');
                modal.nameInput.focus();
            },
            closeEditFarmModal() {
                App.elements.editFarmModal.overlay.classList.remove('show');
            },

            addAmostraCard() {
                const container = App.elements.cigarrinhaAmostragem.amostrasContainer;
                if (!container) return;

                // Recolhe todos os outros cartões antes de adicionar um novo
                container.querySelectorAll('.amostra-card:not(.collapsed)').forEach(c => c.classList.add('collapsed'));

                const amostraId = Date.now();
                const card = document.createElement('div');
                card.className = 'amostra-card'; // Os novos cartões começam expandidos por padrão
                card.dataset.id = amostraId;

                const amostraCount = container.children.length + 1;

                card.innerHTML = `
                    <div class="amostra-header" style="cursor: pointer;">
                        <i class="fas fa-chevron-down amostra-toggle-icon"></i>
                        <h4>Amostra ${amostraCount}</h4>
                        <button type="button" class="btn-remover-amostra" title="Remover Amostra">&times;</button>
                    </div>
                    <div class="amostra-body">
                        <div class="form-row">
                            ${[1, 2, 3, 4, 5].map(i => `
                                <div class="form-col">
                                    <label for="fase${i}-amostra-${amostraId}">Fase ${i}:</label>
                                    <input type="number" id="fase${i}-amostra-${amostraId}" class="amostra-input" min="0" placeholder="0">
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `;
                container.appendChild(card);
                card.querySelector('input').focus();
                App.ui.calculateCigarrinhaAmostragem();
            },

            applyTheme(theme) {
                document.body.className = theme;
                App.elements.userMenu.themeButtons.forEach(btn => {
                    btn.classList.toggle('active', btn.id === theme);
                });
                localStorage.setItem(App.config.themeKey, theme);
                
                Chart.defaults.color = this._getThemeColors().text;

                if (App.state.currentUser && document.getElementById('dashboard').classList.contains('active')) {
                    if(document.getElementById('dashboard-broca').style.display !== 'none') {
                        setTimeout(() => App.charts.renderBrocaDashboardCharts(), 50);
                    }
                    if(document.getElementById('dashboard-perda').style.display !== 'none') {
                        setTimeout(() => App.charts.renderPerdaDashboardCharts(), 50);
                    }
                }
            },
            enableEnterKeyNavigation(formSelector) {
                const form = document.querySelector(formSelector);
                if (!form) return;

                form.addEventListener('keydown', (e) => {
                    if (e.key === 'Enter' && e.target.tagName !== 'TEXTAREA' && e.target.tagName !== 'BUTTON') {
                        e.preventDefault();
                        const fields = Array.from(
                            form.querySelectorAll('input:not([readonly]):not([disabled]), select:not([disabled]), textarea:not([disabled])')
                        );
                        const currentIndex = fields.indexOf(e.target);
                        const nextField = fields[currentIndex + 1];

                        if (nextField) {
                            nextField.focus();
                        } else {
                            form.querySelector('.save, #btnConfirmarOrdemCorte, #btnLogin')?.focus();
                        }
                    }
                });
            },
            _createPermissionItemHTML(perm, permissions = {}) {
                if (!perm.permission) return '';
                const isChecked = permissions[perm.permission];
                return `
                    <label class="permission-item">
                        <input type="checkbox" data-permission="${perm.permission}" ${isChecked ? 'checked' : ''}>
                        <div class="permission-content">
                            <i class="${perm.icon}"></i>
                            <span>${perm.label}</span>
                        </div>
                        <div class="toggle-switch">
                            <span class="slider"></span>
                        </div>
                    </label>
                `;
            },

            renderCompanyModules(containerId) {
                const container = document.getElementById(containerId);
                if (!container) return;
                container.innerHTML = '';

                // Flatten the menu config to get all permissions, excluding superAdmin
                const allPermissions = App.config.menuConfig.flatMap(item =>
                    item.submenu ? item.submenu : [item]
                ).filter(item => item.permission && item.permission !== 'superAdmin');

                allPermissions.forEach(perm => {
                    const checkboxHTML = `
                        <label class="report-option-item">
                            <input type="checkbox" data-module="${perm.permission}" checked>
                            <span class="checkbox-visual"><i class="fas fa-check"></i></span>
                            <span class="option-content">
                                <i class="${perm.icon}"></i>
                                <span>${perm.label}</span>
                            </span>
                        </label>
                    `;
                    container.innerHTML += checkboxHTML;
                });
            },

            renderGlobalFeatures() {
                const grid = document.getElementById('globalFeaturesGrid');
                if (!grid) return;

                grid.innerHTML = ''; // Limpa para re-renderizar
                const allPermissions = App.config.menuConfig.flatMap(item =>
                    item.submenu ? item.submenu : [item]
                ).filter(item => item.permission && item.permission !== 'superAdmin');

                allPermissions.forEach(perm => {
                    const isActive = App.isFeatureGloballyActive(perm.permission);
                    const itemHTML = `
                        <label class="permission-item">
                            <input type="checkbox" data-feature="${perm.permission}" ${isActive ? 'checked' : ''}>
                            <div class="permission-content">
                                <i class="${perm.icon}"></i>
                                <span>${perm.label}</span>
                            </div>
                            <div class="toggle-switch">
                                <span class="slider"></span>
                            </div>
                        </label>
                    `;
                    grid.innerHTML += itemHTML;
                });
            },

            renderPermissionItems(container, permissions = {}, company = null) {
                if (!container) return;
                container.innerHTML = '';

                // Define a lista de módulos permitidos
                let allowedModules = null;
                if (App.state.currentUser.role !== 'super-admin') {
                    const currentCompany = App.state.companies.find(c => c.id === App.state.currentUser.companyId);
                    if (currentCompany && currentCompany.subscribedModules) {
                        allowedModules = new Set(currentCompany.subscribedModules);
                    }
                }

                const allPermissionItems = App.config.menuConfig.flatMap(item =>
                    item.submenu ? item.submenu : [item]
                ).filter(item => item.permission && item.permission !== 'superAdmin');

                // Filtra os itens de permissão com base nos módulos subscritos, se aplicável
                const permissionItemsToRender = allowedModules
                    ? allPermissionItems.filter(perm => allowedModules.has(perm.permission))
                    : allPermissionItems;

                permissionItemsToRender.forEach(perm => {
                    container.innerHTML += this._createPermissionItemHTML(perm, permissions);
                });
            },
            showHistoryFilterModal() {
                const modal = App.elements.historyFilterModal;
                this.populateUserSelects([modal.userSelect]); // Popula apenas o select do modal

                // Set default dates
                const today = new Date();
                const sevenDaysAgo = new Date(today);
                sevenDaysAgo.setDate(today.getDate() - 7);

                modal.startDate.value = sevenDaysAgo.toISOString().split('T')[0];
                modal.endDate.value = today.toISOString().split('T')[0];

                modal.overlay.classList.add('show');
            },
            hideHistoryFilterModal() {
                const modal = App.elements.historyFilterModal;
                modal.overlay.classList.remove('show');
            },

            showSyncHistoryDetailModal() {
                App.elements.syncHistoryDetailModal.overlay.classList.add('show');
            },

            hideSyncHistoryDetailModal() {
                App.elements.syncHistoryDetailModal.overlay.classList.remove('show');
                App.elements.syncHistoryDetailModal.body.innerHTML = ''; // Limpa o conteúdo ao fechar
            },

            hideConfigHistoryModal() {
                const modal = App.elements.configHistoryModal;
                if (modal && modal.overlay) {
                    modal.overlay.classList.remove('show');
                }
            },

            async renderSyncHistoryDetails(logId) {
                const modal = App.elements.syncHistoryDetailModal;
                modal.body.innerHTML = '<div class="spinner-container" style="display:flex; justify-content:center; padding: 20px;"><div class="spinner"></div></div>';
                this.showSyncHistoryDetailModal();

                try {
                    const logDoc = await App.data.getDocument('sync_history_store', logId);

                    if (!logDoc || !logDoc.items || logDoc.items.length === 0) {
                        modal.body.innerHTML = '<p>Nenhum item detalhado encontrado para este registo de sincronização.</p>';
                        return;
                    }

                    const logTimestamp = logDoc.timestamp ? logDoc.timestamp.toDate().toLocaleString('pt-BR') : 'Data não disponível';
                    modal.title.textContent = `Detalhes da Sincronização de ${logTimestamp}`;

                    let contentHTML = '<div class="sync-items-container">';

                    logDoc.items.forEach((item, index) => {
                        const itemStatus = item.status || 'unknown';
                        const cardClass = itemStatus === 'success' ? 'success' : 'failure';
                        const icon = itemStatus === 'success' ? 'fa-check-circle' : 'fa-exclamation-circle';
                        const title = `Item ${index + 1}: ${item.collection}`;

                        let dataDetails = '';
                        if (item.data) {
                            switch (item.collection) {
                                case 'registros': // Broca
                                    dataDetails = `<p><strong>Fazenda/Talhão:</strong> ${item.data.codigo} / ${item.data.talhao}</p><p><strong>Data:</strong> ${item.data.data}</p><p><strong>Índice:</strong> ${item.data.brocamento}%</p>`;
                                    break;
                                case 'perdas':
                                    dataDetails = `<p><strong>Fazenda/Talhão:</strong> ${item.data.codigo} / ${item.data.talhao}</p><p><strong>Data:</strong> ${item.data.data}</p><p><strong>Total:</strong> ${item.data.total} kg</p>`;
                                    break;
                                default:
                                    dataDetails = Object.entries(item.data)
                                        .map(([key, value]) => `<p><strong>${key}:</strong> ${value}</p>`)
                                        .join('');
                                    break;
                            }
                        }

                        const errorInfo = item.status === 'failure'
                            ? `<div class="error-message"><strong>Erro:</strong> ${item.error || 'Desconhecido'}</div>`
                            : '';

                        const retryButton = item.status === 'failure'
                            ? `<div class="sync-item-footer">
                                   <button class="btn-retry-sync" data-action="retry-sync-item" data-item-index="${index}" data-log-id="${logId}">
                                       <i class="fas fa-sync-alt"></i> Tentar Novamente
                                   </button>
                               </div>`
                            : '';

                        contentHTML += `
                            <div class="sync-item-card ${cardClass}" id="sync-item-${index}">
                                <div class="sync-item-header">
                                    <i class="fas ${icon}"></i>
                                    <span>${title}</span>
                                </div>
                                <div class="sync-item-body">
                                    ${dataDetails}
                                    ${errorInfo}
                                </div>
                                ${retryButton}
                            </div>
                        `;
                    });

                    contentHTML += '</div>';
                    modal.body.innerHTML = contentHTML;

                } catch (error) {
                    console.error("Erro ao buscar detalhes do histórico de sincronização:", error);
                    modal.body.innerHTML = '<p style="color: var(--color-danger);">Não foi possível carregar os detalhes.</p>';
                }
            },

            async retrySyncItem(logId, itemIndex) {
                App.ui.setLoading(true, "A tentar sincronizar novamente...");
                try {
                    const logDoc = await App.data.getDocument('sync_history_store', logId);
                    if (!logDoc || !logDoc.items || !logDoc.items[itemIndex]) {
                        throw new Error("Registo de log ou item não encontrado.");
                    }

                    const itemToRetry = logDoc.items[itemIndex];
                    if (itemToRetry.status !== 'failure') {
                        App.ui.showAlert("Este item não falhou, não há necessidade de tentar novamente.", "info");
                        return;
                    }

                    // Tenta adicionar o documento novamente
                    await App.data.addDocument(itemToRetry.collection, itemToRetry.data);

                    // Se for bem-sucedido, atualiza o log no Firestore
                    const updatedItems = [...logDoc.items];
                    updatedItems[itemIndex].status = 'success';
                    updatedItems[itemIndex].error = null; // Limpa a mensagem de erro anterior

                    await App.data.updateDocument('sync_history_store', logId, { items: updatedItems });

                    App.ui.showAlert("Item sincronizado com sucesso!", "success");
                    // Re-renderiza os detalhes para refletir a mudança
                    this.renderSyncHistoryDetails(logId);

                } catch (error) {
                    App.ui.showAlert(`Falha ao tentar novamente: ${error.message}`, "error");
                    console.error("Erro ao tentar sincronizar item novamente:", error);
                } finally {
                    App.ui.setLoading(false);
                }
            },

            setupEventListeners() {
                if (App.elements.btnLogin) App.elements.btnLogin.addEventListener('click', () => App.auth.login());
                const btnOfflineLogin = document.getElementById('btnOfflineLogin');
                if (btnOfflineLogin) {
                    btnOfflineLogin.addEventListener('click', () => {
                        const email = document.getElementById('offlineEmail').value.trim();
                        const password = document.getElementById('offlinePassword').value;
                        App.auth.loginOffline(email, password);
                    });
                }

                // Event Listeners for enabling offline login
                const btnEnableOffline = document.getElementById('btnEnableOfflineLogin');
                if (btnEnableOffline) {
                    btnEnableOffline.addEventListener('click', () => App.ui.showEnableOfflineLoginModal());
                }

                const btnConfirmEnableOffline = document.getElementById('btnConfirmEnableOffline');
                if (btnConfirmEnableOffline) {
                    btnConfirmEnableOffline.addEventListener('click', () => App.actions.enableOfflineLogin());
                }

                const offlineModal = document.getElementById('enableOfflineLoginModal');
                if(offlineModal) {
                    const closeBtn = offlineModal.querySelector('.modal-close-btn');
                    const cancelBtn = offlineModal.querySelector('.btn-cancel');
                    if(closeBtn) closeBtn.addEventListener('click', () => App.ui.closeEnableOfflineLoginModal());
                    if(cancelBtn) cancelBtn.addEventListener('click', () => App.ui.closeEnableOfflineLoginModal());
                    offlineModal.addEventListener('click', e => {
                        if (e.target === offlineModal) App.ui.closeEnableOfflineLoginModal();
                    });
                }
                if (App.elements.logoutBtn) App.elements.logoutBtn.addEventListener('click', () => App.auth.logout());
                if (App.elements.btnToggleMenu) App.elements.btnToggleMenu.addEventListener('click', () => {
                    document.body.classList.toggle('mobile-menu-open');
                    App.elements.menu.classList.toggle('open');
                    App.elements.btnToggleMenu.classList.toggle('open');
                });

                if (App.elements.headerLogo) App.elements.headerLogo.addEventListener('click', () => App.ui.showTab('dashboard'));
                
                document.addEventListener('click', (e) => {
                    if (App.elements.menu && !App.elements.menu.contains(e.target) && App.elements.btnToggleMenu && !App.elements.btnToggleMenu.contains(e.target)) {
                        this.closeAllMenus();
                    }
                    if (App.elements.userMenu.container && !App.elements.userMenu.container.contains(e.target)) {
                        App.elements.userMenu.dropdown.classList.remove('show');
                        App.elements.userMenu.toggle.classList.remove('open');
                        App.elements.userMenu.toggle.setAttribute('aria-expanded', 'false');
                    }
                    if (App.elements.notificationBell.container && !App.elements.notificationBell.container.contains(e.target)) {
                        App.elements.notificationBell.dropdown.classList.remove('show');
                    }
                });

                if (App.elements.userMenu.toggle) App.elements.userMenu.toggle.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const dropdown = App.elements.userMenu.dropdown;
                    const toggle = App.elements.userMenu.toggle;
                    const isShown = dropdown.classList.toggle('show');
                    toggle.classList.toggle('open', isShown);
                    toggle.setAttribute('aria-expanded', isShown);
                    if (App.elements.notificationBell.dropdown) App.elements.notificationBell.dropdown.classList.remove('show');
                });

                if (App.elements.notificationBell.toggle) App.elements.notificationBell.toggle.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const dropdown = App.elements.notificationBell.dropdown;
                    const isShown = dropdown.classList.toggle('show');
                    if (isShown) {
                        App.actions.markNotificationsAsRead();
                    }
                    if (App.elements.userMenu.dropdown) App.elements.userMenu.dropdown.classList.remove('show');
                });

                if (App.elements.notificationBell.clearBtn) App.elements.notificationBell.clearBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    App.actions.clearAllNotifications();
                });

                if (App.elements.notificationBell.list) App.elements.notificationBell.list.addEventListener('click', (e) => {
                    const item = e.target.closest('.notification-item');
                    if (!item) return;

                    const { trapId, logId } = item.dataset;

                    if (trapId) {
                        App.ui.showTab('monitoramentoAereo');
                        App.mapModule.centerOnTrap(trapId);
                        App.elements.notificationBell.dropdown.classList.remove('show');
                    } else if (logId) {
                        App.ui.renderSyncHistoryDetails(logId); // CORREÇÃO FINAL: Chamada para a função no módulo ui
                        App.elements.notificationBell.dropdown.classList.remove('show');
                    }
                });

                if (App.elements.userMenu.themeButtons) App.elements.userMenu.themeButtons.forEach(btn => {
                    btn.addEventListener('click', () => this.applyTheme(btn.id));
                });
                
                const dashEls = App.elements.dashboard;
                if (dashEls.cardBroca) dashEls.cardBroca.addEventListener('click', () => this.showDashboardView('broca'));
                if (dashEls.cardPerda) dashEls.cardPerda.addEventListener('click', () => this.showDashboardView('perda'));
                if (dashEls.cardAerea) dashEls.cardAerea.addEventListener('click', () => this.showDashboardView('aerea'));
                if (dashEls.cardPlantio) dashEls.cardPlantio.addEventListener('click', () => this.showDashboardView('plantio'));
                if (dashEls.cardCigarrinha) dashEls.cardCigarrinha.addEventListener('click', () => this.showDashboardView('cigarrinha'));
                if (dashEls.cardClima) dashEls.cardClima.addEventListener('click', () => this.showDashboardView('clima'));

                if (dashEls.btnBackToSelectorBroca) dashEls.btnBackToSelectorBroca.addEventListener('click', () => this.showDashboardView('selector'));
                if (dashEls.btnBackToSelectorPerda) dashEls.btnBackToSelectorPerda.addEventListener('click', () => this.showDashboardView('selector'));
                if (dashEls.btnBackToSelectorAerea) dashEls.btnBackToSelectorAerea.addEventListener('click', () => this.showDashboardView('selector'));
                if (dashEls.btnBackToSelectorPlantio) dashEls.btnBackToSelectorPlantio.addEventListener('click', () => this.showDashboardView('selector'));
                if (dashEls.btnBackToSelectorCigarrinha) dashEls.btnBackToSelectorCigarrinha.addEventListener('click', () => this.showDashboardView('selector'));
                if (dashEls.btnBackToSelectorClima) dashEls.btnBackToSelectorClima.addEventListener('click', () => this.showDashboardView('selector'));

                const btnSavePlantingGoals = document.getElementById('btnSavePlantingGoals');
                if (btnSavePlantingGoals) {
                    btnSavePlantingGoals.addEventListener('click', () => App.actions.savePlantingGoals());
                }

                if (dashEls.btnFiltrarBrocaDashboard) dashEls.btnFiltrarBrocaDashboard.addEventListener('click', () => App.charts.renderBrocaDashboardCharts());
                if (dashEls.btnFiltrarPerdaDashboard) dashEls.btnFiltrarPerdaDashboard.addEventListener('click', () => App.charts.renderPerdaDashboardCharts());
                if (document.getElementById('btnFiltrarPlantioDashboard')) {
                    document.getElementById('btnFiltrarPlantioDashboard').addEventListener('click', () => App.charts.renderPlantioDashboardCharts());
                }
                if (document.getElementById('btnFiltrarAereoDashboard')) {
                    document.getElementById('btnFiltrarAereoDashboard').addEventListener('click', () => App.charts.renderAereoDashboardCharts());
                }
                if (document.getElementById('btnFiltrarCigarrinhaDashboard')) {
                    document.getElementById('btnFiltrarCigarrinhaDashboard').addEventListener('click', () => App.charts.renderCigarrinhaDashboardCharts());
                }

                if (document.getElementById('btnFiltrarClimaDashboard')) {
                    document.getElementById('btnFiltrarClimaDashboard').addEventListener('click', () => App.charts.renderClimaDashboardCharts());
                }
                
                const chartModal = App.elements.chartModal;
                if (chartModal.closeBtn) chartModal.closeBtn.addEventListener('click', () => App.charts.closeChartModal());
                if (chartModal.overlay) chartModal.overlay.addEventListener('click', e => { if(e.target === chartModal.overlay) App.charts.closeChartModal(); });
                
                document.addEventListener('click', (e) => {
                    if (e.target.closest('.btn-expand-chart')) {
                        const button = e.target.closest('.btn-expand-chart');
                        App.charts.openChartModal(button.dataset.chartId);
                    }
                });

                if (App.elements.users.role) App.elements.users.role.addEventListener('change', (e) => this.updatePermissionsForRole(e.target.value));
                
                if (App.elements.users.btnCreate) App.elements.users.btnCreate.addEventListener('click', () => App.auth.initiateUserCreation());
                
                if (App.elements.users.list) App.elements.users.list.addEventListener('click', e => {
                    const button = e.target.closest('button[data-action]');
                    if (!button) return;
                    const { action, id } = button.dataset;
                    if (action === 'edit') this.openUserEditModal(id);
                    if (action === 'toggle') App.auth.toggleUserStatus(id);
                });

                const adminModal = App.elements.adminPasswordConfirmModal;
                if (adminModal.closeBtn) adminModal.closeBtn.addEventListener('click', () => this.closeAdminPasswordConfirmModal());
                if (adminModal.cancelBtn) adminModal.cancelBtn.addEventListener('click', () => this.closeAdminPasswordConfirmModal());
                if (adminModal.confirmBtn) adminModal.confirmBtn.addEventListener('click', () => App.auth.executeAdminAction());
                if (adminModal.overlay) adminModal.overlay.addEventListener('click', e => { if(e.target === adminModal.overlay) this.closeAdminPasswordConfirmModal(); });


                const modalEls = App.elements.userEditModal;
                if (modalEls.closeBtn) modalEls.closeBtn.addEventListener('click', () => this.closeUserEditModal());
                if (modalEls.overlay) modalEls.overlay.addEventListener('click', e => { if(e.target === modalEls.overlay) this.closeUserEditModal(); });
                if (modalEls.btnSaveChanges) modalEls.btnSaveChanges.addEventListener('click', () => App.auth.saveUserChanges(modalEls.editingUserId.value));
                if (modalEls.btnResetPassword) modalEls.btnResetPassword.addEventListener('click', () => App.auth.resetUserPassword(modalEls.editingUserId.value));
                if (modalEls.btnDeleteUser) modalEls.btnDeleteUser.addEventListener('click', () => App.auth.deleteUser(modalEls.editingUserId.value));
                if (modalEls.role) modalEls.role.addEventListener('change', (e) => this.updatePermissionsForRole(e.target.value, '#editUserPermissionGrid'));
                
                const companyEls = App.elements.companyManagement;
                if (companyEls.btnCreate) companyEls.btnCreate.addEventListener('click', () => App.actions.createCompany());

                const btnSaveGlobalFeatures = document.getElementById('btnSaveGlobalFeatures');
                if (btnSaveGlobalFeatures) {
                    btnSaveGlobalFeatures.addEventListener('click', () => App.actions.saveGlobalFeatures());
                }

                if (companyEls.list) companyEls.list.addEventListener('click', e => {
                    const button = e.target.closest('button[data-action]');
                    if (!button) return;
                    const { action, id } = button.dataset;
                    if (action === 'edit-company') this.openEditCompanyModal(id);
                    if (action === 'toggle-company') App.actions.toggleCompanyStatus(id);
                    if (action === 'delete-company-permanently') App.actions.deleteCompanyPermanently(id);
                    if (action === 'view-as-company') App.actions.impersonateCompany(id);
                });

                const editCompanyModalEls = App.elements.editCompanyModal;
                if (editCompanyModalEls.closeBtn) editCompanyModalEls.closeBtn.addEventListener('click', () => this.closeEditCompanyModal());
                if (editCompanyModalEls.cancelBtn) editCompanyModalEls.cancelBtn.addEventListener('click', () => this.closeEditCompanyModal());
                if (editCompanyModalEls.saveBtn) editCompanyModalEls.saveBtn.addEventListener('click', () => App.actions.saveCompanyModuleChanges());
                if (editCompanyModalEls.overlay) editCompanyModalEls.overlay.addEventListener('click', e => { if (e.target === editCompanyModalEls.overlay) this.closeEditCompanyModal(); });

                const btnMigrate = document.getElementById('btnMigrateOldData');
                if (btnMigrate) btnMigrate.addEventListener('click', () => App.actions.migrateOldData());
                
                const cpModal = App.elements.changePasswordModal;
                if (App.elements.userMenu.changePasswordBtn) App.elements.userMenu.changePasswordBtn.addEventListener('click', () => cpModal.overlay.classList.add('show'));
                if (App.elements.userMenu.manualSyncBtn) App.elements.userMenu.manualSyncBtn.addEventListener('click', () => App.actions.forceTokenRefresh(true));
                if (cpModal.closeBtn) cpModal.closeBtn.addEventListener('click', () => cpModal.overlay.classList.remove('show'));
                if (cpModal.cancelBtn) cpModal.cancelBtn.addEventListener('click', () => cpModal.overlay.classList.remove('show'));
                if (cpModal.saveBtn) cpModal.saveBtn.addEventListener('click', () => App.actions.changePassword());


                if (App.elements.personnel.btnSave) App.elements.personnel.btnSave.addEventListener('click', () => App.actions.savePersonnel());
                if (App.elements.personnel.list) App.elements.personnel.list.addEventListener('click', e => {
                    const btn = e.target.closest('button');
                    if (!btn) return;
                    const { action, id } = btn.dataset;
                    if (action === 'edit-personnel') App.actions.editPersonnel(id);
                    if (action === 'delete-personnel') App.actions.deletePersonnel(id);
                });
                if (App.elements.personnel.csvUploadArea) App.elements.personnel.csvUploadArea.addEventListener('click', () => App.elements.personnel.csvFileInput.click());
                if (App.elements.personnel.csvFileInput) App.elements.personnel.csvFileInput.addEventListener('change', (e) => App.actions.importPersonnelFromCSV(e.target.files[0]));
                if (App.elements.personnel.btnDownloadCsvTemplate) App.elements.personnel.btnDownloadCsvTemplate.addEventListener('click', () => App.actions.downloadPersonnelCsvTemplate());
                
                const companyConfigEls = App.elements.companyConfig;
                if (companyConfigEls.logoUploadArea) companyConfigEls.logoUploadArea.addEventListener('click', () => companyConfigEls.logoInput.click());
                if (companyConfigEls.logoInput) companyConfigEls.logoInput.addEventListener('change', (e) => App.actions.handleLogoUpload(e));
                if (companyConfigEls.removeLogoBtn) companyConfigEls.removeLogoBtn.addEventListener('click', () => App.actions.removeLogo());
                if (companyConfigEls.progressUploadArea) companyConfigEls.progressUploadArea.addEventListener('click', () => companyConfigEls.progressInput.click());
                if (companyConfigEls.progressInput) companyConfigEls.progressInput.addEventListener('change', (e) => App.actions.importHarvestReport(e.target.files[0], 'progress'));
                if (companyConfigEls.btnDownloadProgressTemplate) companyConfigEls.btnDownloadProgressTemplate.addEventListener('click', () => App.actions.downloadHarvestReportTemplate('progress'));
                if (companyConfigEls.closedUploadArea) companyConfigEls.closedUploadArea.addEventListener('click', () => companyConfigEls.closedInput.click());
                if (companyConfigEls.closedInput) companyConfigEls.closedInput.addEventListener('change', (e) => App.actions.importHarvestReport(e.target.files[0], 'closed'));
                if (companyConfigEls.btnDownloadClosedTemplate) companyConfigEls.btnDownloadClosedTemplate.addEventListener('click', () => App.actions.downloadHarvestReportTemplate('closed'));
                if (companyConfigEls.shapefileUploadArea) companyConfigEls.shapefileUploadArea.addEventListener('click', () => companyConfigEls.shapefileInput.click());
                if (companyConfigEls.shapefileInput) companyConfigEls.shapefileInput.addEventListener('change', (e) => App.mapModule.handleShapefileUpload(e));

                const btnCleanDuplicateTraps = document.getElementById('btnCleanDuplicateTraps');
                if (btnCleanDuplicateTraps) {
                    btnCleanDuplicateTraps.addEventListener('click', () => App.actions.deduplicateTraps());
                }

                // Event listeners for historical report upload
                if (companyConfigEls.btnDownloadHistoricalTemplate) {
                    companyConfigEls.btnDownloadHistoricalTemplate.addEventListener('click', () => App.actions.downloadHistoricalReportTemplate());
                }
                if (companyConfigEls.btnDeleteHistoricalData) {
                    companyConfigEls.btnDeleteHistoricalData.addEventListener('click', () => App.actions.deleteHistoricalData());
                }
                if (companyConfigEls.historicalReportUploadArea) {
                    const uploadArea = companyConfigEls.historicalReportUploadArea;
                    const input = companyConfigEls.historicalReportInput;

                    uploadArea.addEventListener('click', () => input.click());
                    input.addEventListener('change', (e) => App.actions.uploadHistoricalReport(e.target.files[0]));

                    uploadArea.addEventListener('dragover', (e) => {
                        e.preventDefault();
                        uploadArea.classList.add('dragover');
                    });

                    uploadArea.addEventListener('dragleave', () => {
                        uploadArea.classList.remove('dragover');
                    });

                    uploadArea.addEventListener('drop', (e) => {
                        e.preventDefault();
                        uploadArea.classList.remove('dragover');
                        if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
                            App.actions.uploadHistoricalReport(e.dataTransfer.files[0]);
                            input.files = e.dataTransfer.files; // Optional: syncs the file list
                        }
                    });
                }


                if (App.elements.cadastros.btnSaveFarm) App.elements.cadastros.btnSaveFarm.addEventListener('click', () => App.actions.saveFarm());
                if (App.elements.cadastros.btnDeleteAllFarms) App.elements.cadastros.btnDeleteAllFarms.addEventListener('click', () => App.actions.deleteAllFarms());
                if (App.elements.cadastros.farmSelect) App.elements.cadastros.farmSelect.addEventListener('change', (e) => this.renderTalhaoList(e.target.value));
                
                if (App.elements.cadastros.talhaoManagementContainer) App.elements.cadastros.talhaoManagementContainer.addEventListener('click', e => { 
                    const btn = e.target.closest('button[data-action]'); 
                    if(!btn) return; 
                    const { action, id } = btn.dataset; 
                    if(action === 'edit-talhao') App.actions.editTalhao(id); 
                    if(action === 'delete-talhao') App.actions.deleteTalhao(id);
                    if(action === 'edit-farm') this.openEditFarmModal(id);
                    if(action === 'delete-farm') App.actions.deleteFarm(id);
                });

                if (App.elements.cadastros.btnSaveTalhao) App.elements.cadastros.btnSaveTalhao.addEventListener('click', () => App.actions.saveTalhao());
                if (App.elements.cadastros.csvUploadArea) App.elements.cadastros.csvUploadArea.addEventListener('click', () => App.elements.cadastros.csvFileInput.click());
                if (App.elements.cadastros.csvFileInput) App.elements.cadastros.csvFileInput.addEventListener('change', (e) => App.actions.importFarmsFromCSV(e.target.files[0]));
                if (App.elements.cadastros.btnDownloadCsvTemplate) App.elements.cadastros.btnDownloadCsvTemplate.addEventListener('click', () => App.actions.downloadCsvTemplate());
                if (App.elements.cadastros.talhaoArea) App.elements.cadastros.talhaoArea.addEventListener('input', App.actions.calculateTalhaoProducao);
                if (App.elements.cadastros.talhaoTCH) App.elements.cadastros.talhaoTCH.addEventListener('input', App.actions.calculateTalhaoProducao);
                
                const editFarmModalEls = App.elements.editFarmModal;
                if (editFarmModalEls.closeBtn) editFarmModalEls.closeBtn.addEventListener('click', () => this.closeEditFarmModal());
                if (editFarmModalEls.cancelBtn) editFarmModalEls.cancelBtn.addEventListener('click', () => this.closeEditFarmModal());
                if (editFarmModalEls.saveBtn) editFarmModalEls.saveBtn.addEventListener('click', () => App.actions.saveFarmChanges());

                if (App.elements.planejamento.btnAgendar) App.elements.planejamento.btnAgendar.addEventListener('click', () => App.actions.agendarInspecao());
                if (App.elements.planejamento.btnSugerir) App.elements.planejamento.btnSugerir.addEventListener('click', () => App.gemini.getPlanningSuggestions());
                if (App.elements.planejamento.lista) App.elements.planejamento.lista.addEventListener('click', (e) => { const button = e.target.closest('button[data-action]'); if(!button) return; const { action, id } = button.dataset; if (action === 'concluir') App.actions.marcarPlanoComoConcluido(id); if (action === 'excluir') App.actions.excluirPlano(id); });
                
                const harvestEls = App.elements.harvest;
                if (harvestEls.btnAddNew) harvestEls.btnAddNew.addEventListener('click', () => App.actions.editHarvestPlan());
                if (harvestEls.btnCancelPlan) harvestEls.btnCancelPlan.addEventListener('click', () => this.showHarvestPlanList());
                if (harvestEls.btnSavePlan) harvestEls.btnSavePlan.addEventListener('click', () => App.actions.saveHarvestPlan());
                if (harvestEls.plansList) harvestEls.plansList.addEventListener('click', (e) => {
                    const button = e.target.closest('button[data-action]');
                    if (!button) return;
                    const { action, id } = button.dataset;
                    if (action === 'edit') App.actions.editHarvestPlan(id);
                    if (action === 'delete') App.actions.deleteHarvestPlan(id);
                });
                if (harvestEls.fazenda) harvestEls.fazenda.addEventListener('change', e => this.renderHarvestTalhaoSelection(e.target.value));
                
                if (harvestEls.selectAllTalhoes) harvestEls.selectAllTalhoes.addEventListener('change', (e) => {
                    const isChecked = e.target.checked;
                    const talhaoCheckboxes = App.elements.harvest.talhaoSelectionList.querySelectorAll('input[type="checkbox"]');
                    talhaoCheckboxes.forEach(cb => {
                        if (!cb.disabled) {
                            cb.checked = isChecked;
                        }
                    });
                    if (isChecked) {
                        App.elements.harvest.btnAddOrUpdate.click();
                    }
                });

                if (harvestEls.btnAddOrUpdate) harvestEls.btnAddOrUpdate.addEventListener('click', () => App.actions.addOrUpdateHarvestSequence());
                if (harvestEls.btnCancelEdit) harvestEls.btnCancelEdit.addEventListener('click', () => App.actions.cancelEditSequence());
                if (harvestEls.btnOptimize) {
                    harvestEls.btnOptimize.innerHTML = `<i class="fas fa-brain"></i> Otimizar Colheita`;
                    harvestEls.btnOptimize.addEventListener('click', () => App.gemini.getOptimizedHarvestSequence());
                }

                const debouncedAtrPrediction = App.debounce(() => App.actions.getAtrPrediction());
                if (harvestEls.fazenda) harvestEls.fazenda.addEventListener('change', debouncedAtrPrediction);

                if (harvestEls.tableBody) {
                    harvestEls.tableBody.addEventListener('click', e => {
                        const removeBtn = e.target.closest('button[data-action="remove-harvest"]');
                        if (removeBtn) App.actions.removeHarvestSequence(removeBtn.dataset.id);
                        const editBtn = e.target.closest('button[data-action="edit-harvest-group"]');
                        if(editBtn) App.actions.editHarvestSequenceGroup(editBtn.dataset.id);
                        const atrSpan = e.target.closest('.editable-atr');
                        if (atrSpan) {
                            // A função de edição foi removida a pedido.
                        }
                    });
                    [harvestEls.frontName, harvestEls.startDate, harvestEls.dailyRate].forEach(el => {
                        if(el) el.addEventListener('input', () => App.actions.updateActiveHarvestPlanDetails())
                    });
                
                    let dragSrcEl = null;
                    harvestEls.tableBody.addEventListener('dragstart', e => { dragSrcEl = e.target; e.dataTransfer.effectAllowed = 'move'; e.dataTransfer.setData('text/html', e.target.innerHTML); });
                    harvestEls.tableBody.addEventListener('dragover', e => { e.preventDefault(); return false; });
                    harvestEls.tableBody.addEventListener('drop', e => { e.stopPropagation(); if (dragSrcEl !== e.target) { const targetRow = e.target.closest('tr'); if(targetRow) App.actions.reorderHarvestSequence(dragSrcEl.dataset.id, targetRow.dataset.id); } return false; });
                }
                
                if (App.elements.broca.codigo) App.elements.broca.codigo.addEventListener('change', () => App.actions.findVarietyForTalhao('broca'));
                if (App.elements.broca.talhao) App.elements.broca.talhao.addEventListener('input', () => App.actions.findVarietyForTalhao('broca'));
                ['brocaBase', 'brocaMeio', 'brocaTopo', 'entrenos'].forEach(id => {
                    const el = document.getElementById(id);
                    if (el) el.addEventListener('input', () => {
                        App.ui.updateBrocadoTotal();
                        App.ui.calculateBrocamento();
                    });
                });
                
                if (App.elements.perda.codigo) App.elements.perda.codigo.addEventListener('change', () => App.actions.findVarietyForTalhao('perda'));
                if (App.elements.perda.talhao) App.elements.perda.talhao.addEventListener('input', () => App.actions.findVarietyForTalhao('perda'));
                if (App.elements.perda.matricula) App.elements.perda.matricula.addEventListener('input', () => App.actions.findOperatorName());
                ['canaInteira', 'tolete', 'toco', 'ponta', 'estilhaco', 'pedaco'].forEach(id => {
                    const el = document.getElementById(id);
                    if (el) el.addEventListener('input', () => App.ui.calculatePerda());
                });
                
                if (App.elements.broca.btnSalvar) App.elements.broca.btnSalvar.addEventListener('click', () => App.actions.saveBrocamento());
                if (App.elements.perda.btnSalvar) App.elements.perda.btnSalvar.addEventListener('click', () => App.actions.savePerda());
                if (App.elements.cigarrinha.btnSalvar) App.elements.cigarrinha.btnSalvar.addEventListener('click', () => App.actions.saveCigarrinha());

                if (App.elements.cigarrinha.codigo) App.elements.cigarrinha.codigo.addEventListener('change', () => App.actions.findVarietyForTalhao('cigarrinha'));
                if (App.elements.cigarrinha.talhao) App.elements.cigarrinha.talhao.addEventListener('input', () => App.actions.findVarietyForTalhao('cigarrinha'));
                ['fase1', 'fase2', 'fase3', 'fase4', 'fase5'].forEach(id => {
                    const el = App.elements.cigarrinha[id];
                    if (el) el.addEventListener('input', () => App.ui.calculateCigarrinha());
                });
                
                if (App.elements.broca.btnPDF) App.elements.broca.btnPDF.addEventListener('click', () => App.reports.generateBrocamentoPDF());
                if (App.elements.broca.btnExcel) App.elements.broca.btnExcel.addEventListener('click', () => App.reports.generateBrocamentoCSV());
                if (App.elements.perda.btnPDF) App.elements.perda.btnPDF.addEventListener('click', () => App.reports.generatePerdaPDF());
                if (App.elements.perda.btnExcel) App.elements.perda.btnExcel.addEventListener('click', () => App.reports.generatePerdaCSV());
                if (App.elements.cigarrinha.btnPDF) App.elements.cigarrinha.btnPDF.addEventListener('click', () => App.reports.generateCigarrinhaPDF());
                if (App.elements.cigarrinha.btnExcel) App.elements.cigarrinha.btnExcel.addEventListener('click', () => App.reports.generateCigarrinhaCSV());

                // Listeners para Cigarrinha Amostragem
                const amostragemEls = App.elements.cigarrinhaAmostragem;
                if (amostragemEls.addAmostraBtn) {
                    amostragemEls.addAmostraBtn.addEventListener('click', () => App.ui.addAmostraCard());
                }
                if (amostragemEls.amostrasContainer) {
                    amostragemEls.amostrasContainer.addEventListener('click', e => {
                        const header = e.target.closest('.amostra-header');
                        const removeBtn = e.target.closest('.btn-remover-amostra');

                        if (removeBtn) {
                            e.stopPropagation(); // Impede que o clique no botão de remover também acione o colapso
                            removeBtn.closest('.amostra-card').remove();
                            // Renumera os cartões para uma melhor experiência do utilizador
                            const allCards = amostragemEls.amostrasContainer.querySelectorAll('.amostra-card');
                            allCards.forEach((card, index) => {
                                card.querySelector('h4').textContent = `Amostra ${index + 1}`;
                            });
                            App.ui.calculateCigarrinhaAmostragem();
                        } else if (header) {
                            header.closest('.amostra-card').classList.toggle('collapsed');
                        }
                    });

                    amostragemEls.amostrasContainer.addEventListener('input', e => {
                        if (e.target.classList.contains('amostra-input')) {
                            App.ui.calculateCigarrinhaAmostragem();
                        }
                    });
                }
                if (amostragemEls.codigo) amostragemEls.codigo.addEventListener('change', () => App.actions.findVarietyForTalhao('cigarrinhaAmostragem'));
                if (amostragemEls.talhao) amostragemEls.talhao.addEventListener('input', () => App.actions.findVarietyForTalhao('cigarrinhaAmostragem'));
                if (amostragemEls.btnSalvar) amostragemEls.btnSalvar.addEventListener('click', () => App.actions.saveCigarrinhaAmostragem());
                if (amostragemEls.btnPDF) amostragemEls.btnPDF.addEventListener('click', () => App.reports.generateCigarrinhaAmostragemPDF());
                if (amostragemEls.btnExcel) amostragemEls.btnExcel.addEventListener('click', () => App.reports.generateCigarrinhaAmostragemCSV());

                if (App.elements.gerenciamento.lista) App.elements.gerenciamento.lista.addEventListener('click', e => {
                    const button = e.target.closest('button');
                    if (button) {
                        const action = button.dataset.action;
                        const type = button.dataset.type;
                        const id = button.dataset.id;
                        if (action === 'delete') {
                            App.actions.deleteEntry(type, id);
                        } else if (action === 'edit') {
                            App.actions.editEntry(type, id);
                        }
                    }
                });
                if (App.elements.gerenciamento.applyBtn) App.elements.gerenciamento.applyBtn.addEventListener('click', () => this.renderGerenciamento());
                
                const customReportEls = App.elements.relatorioColheita;
                if (customReportEls.btnPDF) customReportEls.btnPDF.addEventListener('click', () => App.reports.generateCustomHarvestReport('pdf'));
                if (customReportEls.btnExcel) customReportEls.btnExcel.addEventListener('click', () => App.reports.generateCustomHarvestReport('csv'));
                if (customReportEls.tipoRelatorioSelect) customReportEls.tipoRelatorioSelect.addEventListener('change', (e) => {
                    const isDetalhado = e.target.value === 'detalhado';
                    if (customReportEls.colunasDetalhadoContainer) customReportEls.colunasDetalhadoContainer.style.display = isDetalhado ? 'block' : 'none';
                });
                
                const monitoramentoAereoEls = App.elements.monitoramentoAereo;
                if (monitoramentoAereoEls.infoBoxCloseBtn) monitoramentoAereoEls.infoBoxCloseBtn.addEventListener('click', () => App.mapModule.hideTalhaoInfo());
                if (monitoramentoAereoEls.trapInfoBoxCloseBtn) monitoramentoAereoEls.trapInfoBoxCloseBtn.addEventListener('click', () => App.mapModule.hideTrapInfo());
                
                // Listeners for the main map controls, added here to prevent re-binding
                if (monitoramentoAereoEls.btnAddTrap) monitoramentoAereoEls.btnAddTrap.addEventListener('click', () => {
                    if (App.state.trapPlacementMode === 'manual_select') {
                        App.state.trapPlacementMode = null;
                        App.ui.showAlert("Seleção manual cancelada.", "info");
                    } else {
                        App.mapModule.promptInstallTrap();
                    }
                });
                if (monitoramentoAereoEls.btnCenterMap) monitoramentoAereoEls.btnCenterMap.addEventListener('click', () => App.mapModule.centerMapOnUser());
                if (monitoramentoAereoEls.btnHistory) monitoramentoAereoEls.btnHistory.addEventListener('click', () => this.showHistoryFilterModal());
                if (monitoramentoAereoEls.btnToggleRiskView) monitoramentoAereoEls.btnToggleRiskView.addEventListener('click', () => App.mapModule.toggleRiskView());

                const trapModal = App.elements.trapPlacementModal;
                if (trapModal.closeBtn) trapModal.closeBtn.addEventListener('click', () => App.mapModule.hideTrapPlacementModal());
                if (trapModal.cancelBtn) trapModal.cancelBtn.addEventListener('click', () => App.mapModule.hideTrapPlacementModal());
                if (trapModal.manualBtn) trapModal.manualBtn.addEventListener('click', () => {
                    const userRole = App.state.currentUser.role;
                    if (userRole !== 'admin' && userRole !== 'super-admin') {
                        App.ui.showAlert("Apenas administradores podem instalar armadilhas manualmente.", "error");
                        return;
                    }

                    // Define a ação a ser executada após a confirmação da senha
                    const manualPlacementAction = () => {
                        App.mapModule.hideTrapPlacementModal();
                        App.state.trapPlacementMode = 'manual_select';
                        App.ui.showAlert("Modo de seleção manual ativado. Clique no talhão desejado no mapa.", "info", 4000);
                    };

                    // Armazena a ação e mostra o modal de senha
                    App.state.adminAction = manualPlacementAction;
                    App.ui.showAdminPasswordConfirmModal();
                });
                if (trapModal.confirmBtn) trapModal.confirmBtn.addEventListener('click', () => {
                    const { trapPlacementMode, trapPlacementData, mapboxUserMarker } = App.state;

                    if (!mapboxUserMarker && trapPlacementMode !== 'manual_confirm') {
                        App.ui.showAlert("Localização do usuário não disponível.", "error");
                        return;
                    }

                    let selectedFeature = null;
                    let installPosition = null;

                    if (trapPlacementMode === 'success') {
                        selectedFeature = trapPlacementData.feature;
                        installPosition = mapboxUserMarker.getLngLat();
                    } else if (trapPlacementMode === 'conflict') {
                        const selectedRadio = document.querySelector('input[name="talhaoConflict"]:checked');
                        if (selectedRadio) {
                            const selectedIndex = parseInt(selectedRadio.value, 10);
                            selectedFeature = trapPlacementData.features[selectedIndex];
                            installPosition = mapboxUserMarker.getLngLat();
                        } else {
                            App.ui.showAlert("Por favor, selecione um talhão.", "warning");
                            return;
                        }
                    } else if (trapPlacementMode === 'manual_confirm') {
                        selectedFeature = trapPlacementData.feature;
                        installPosition = trapPlacementData.position;
                    }

                    if (selectedFeature && installPosition) {
                        App.mapModule.installTrap(installPosition.lat, installPosition.lng, selectedFeature);
                        App.mapModule.hideTrapPlacementModal();
                    }
                });

                const relatorioMonitoramentoEls = App.elements.relatorioMonitoramento;
                if (relatorioMonitoramentoEls.btnPDF) relatorioMonitoramentoEls.btnPDF.addEventListener('click', () => App.reports.generateArmadilhaPDF());
                if (relatorioMonitoramentoEls.btnExcel) relatorioMonitoramentoEls.btnExcel.addEventListener('click', () => App.reports.generateArmadilhaCSV());
                
                if (App.elements.notificationContainer) App.elements.notificationContainer.addEventListener('click', (e) => {
                    const notification = e.target.closest('.trap-notification');
                    if (notification && notification.dataset.trapId) {
                        App.mapModule.centerOnTrap(notification.dataset.trapId);
                    }
                });

                this.enableEnterKeyNavigation('#loginBox');
                this.enableEnterKeyNavigation('#lancamentoBroca');
                if (App.elements.frenteDePlantio.btnSave) App.elements.frenteDePlantio.btnSave.addEventListener('click', () => App.actions.saveFrenteDePlantio());
                if (App.elements.frenteDePlantio.list) App.elements.frenteDePlantio.list.addEventListener('click', e => {
                    const btn = e.target.closest('button');
                    if (!btn) return;
                    const { action, id } = btn.dataset;
                    if (action === 'edit-frente') App.actions.editFrenteDePlantio(id);
                    if (action === 'delete-frente') App.actions.deleteFrenteDePlantio(id);
                });

                // Listeners for Apontamento de Plantio
                const apontamentoEls = App.elements.apontamentoPlantio;
                if (apontamentoEls.addRecordBtn) {
                    apontamentoEls.addRecordBtn.addEventListener('click', () => App.ui.addPlantioRecordCard());
                }
                if (apontamentoEls.btnSave) {
                    apontamentoEls.btnSave.addEventListener('click', () => App.actions.saveApontamentoPlantio());
                }
                if (apontamentoEls.recordsContainer) {
                    apontamentoEls.recordsContainer.addEventListener('click', e => {
                        const header = e.target.closest('.amostra-header');
                        const removeBtn = e.target.closest('.btn-remover-amostra');

                        if (removeBtn) {
                            e.stopPropagation();
                            removeBtn.closest('.amostra-card').remove();
                            const allCards = apontamentoEls.recordsContainer.querySelectorAll('.amostra-card');
                            allCards.forEach((card, index) => {
                                card.querySelector('h4').textContent = `Lançamento ${index + 1}`;
                            });
                            App.ui.calculateTotalPlantedArea();
                        } else if (header) {
                            header.closest('.amostra-card').classList.toggle('collapsed');
                        }
                    });
                    apontamentoEls.recordsContainer.addEventListener('input', e => {
                        if (e.target.classList.contains('plantio-area-input')) {
                            App.ui.calculateTotalPlantedArea();
                        }
                    });
                }
                if (apontamentoEls.frente) {
                    apontamentoEls.frente.addEventListener('change', e => {
                        const frenteId = e.target.value;
                        const frente = App.state.frentesDePlantio.find(f => f.id === frenteId);
                        apontamentoEls.provider.value = frente ? frente.provider : '';
                    });
                }

                if (apontamentoEls.leaderId) {
                    apontamentoEls.leaderId.addEventListener('input', () => App.actions.findLeaderName());
                }

                if (apontamentoEls.farmName) {
                    apontamentoEls.farmName.addEventListener('change', () => this.updateAllTalhaoSelects());
                }

                // Listeners for Apontamento Climatológico
                const climaEls = App.elements.lancamentoClima;
                if (climaEls && climaEls.btnSave) {
                    climaEls.btnSave.addEventListener('click', () => App.actions.saveLancamentoClima());
                }
                if (climaEls && climaEls.fazenda) {
                    climaEls.fazenda.addEventListener('change', (e) => {
                        const farmId = e.target.value;
                        const farm = App.state.fazendas.find(f => f.id === farmId);
                        const talhaoSelect = climaEls.talhao;
                        talhaoSelect.innerHTML = '<option value="">Selecione...</option>';
                        if (farm && farm.talhoes) {
                            farm.talhoes.forEach(talhao => {
                                talhaoSelect.innerHTML += `<option value="${talhao.name}">${talhao.name}</option>`;
                            });
                        }
                    });
                }


                this.enableEnterKeyNavigation('#loginBox');
                this.enableEnterKeyNavigation('#lancamentoBroca');
                this.enableEnterKeyNavigation('#lancamentoPerda');
                this.enableEnterKeyNavigation('#lancamentoCigarrinha');
                this.enableEnterKeyNavigation('#lancamentoCigarrinhaAmostragem');
                this.enableEnterKeyNavigation('#frenteDePlantio');
                this.enableEnterKeyNavigation('#apontamentoPlantio');
                this.enableEnterKeyNavigation('#relatorioPlantio');
                this.enableEnterKeyNavigation('#lancamentoClima');

                const relatorioPlantioEls = App.elements.relatorioPlantio;
                if (relatorioPlantioEls.btnPDF) relatorioPlantioEls.btnPDF.addEventListener('click', () => {
                    const reportType = relatorioPlantioEls.tipo.value;
                    if (reportType === 'fazenda') {
                        App.reports.generatePlantioFazendaPDF();
                    } else {
                        App.reports.generatePlantioTalhaoPDF();
                    }
                });
                if (relatorioPlantioEls.btnExcel) relatorioPlantioEls.btnExcel.addEventListener('click', () => {
                    const reportType = relatorioPlantioEls.tipo.value;
                    if (reportType === 'fazenda') {
                        App.reports.generatePlantioFazendaExcel();
                    } else {
                        App.reports.generatePlantioTalhaoExcel();
                    }
                });

                const relatorioClimaEls = App.elements.relatorioClima;
                if (relatorioClimaEls && relatorioClimaEls.btnPDF) {
                    relatorioClimaEls.btnPDF.addEventListener('click', () => App.reports.generateClimaPDF());
                }
                if (relatorioClimaEls && relatorioClimaEls.btnExcel) {
                    relatorioClimaEls.btnExcel.addEventListener('click', () => App.reports.generateClimaCSV());
                }

                const relatorioRiscoEls = App.elements.relatorioRisco;
                if (relatorioRiscoEls.btnPDF) relatorioRiscoEls.btnPDF.addEventListener('click', () => App.reports.generateRiskViewPDF());
                if (relatorioRiscoEls.btnExcel) relatorioRiscoEls.btnExcel.addEventListener('click', () => App.reports.generateRiskViewCSV());


                this.enableEnterKeyNavigation('#changePasswordModal');
                this.enableEnterKeyNavigation('#cadastros');
                this.enableEnterKeyNavigation('#cadastrarPessoas');
                this.enableEnterKeyNavigation('#adminPasswordConfirmModal');

                ['mousemove', 'mousedown', 'keypress', 'scroll', 'touchstart'].forEach(event => {
                    document.addEventListener(event, () => App.actions.resetInactivityTimer());
                });

                if (App.elements.installAppBtn) App.elements.installAppBtn.addEventListener('click', async () => {
                    if (App.state.deferredInstallPrompt) {
                        App.state.deferredInstallPrompt.prompt();
                        const { outcome } = await App.state.deferredInstallPrompt.userChoice;
                        console.log(`User response to the install prompt: ${outcome}`);
                        App.state.deferredInstallPrompt = null;
                        App.elements.installAppBtn.style.display = 'none';
                    }
                });

                const historyModal = App.elements.historyFilterModal;
                if (historyModal.overlay) historyModal.overlay.addEventListener('click', e => { if(e.target === historyModal.overlay) this.hideHistoryFilterModal(); });
                if (historyModal.closeBtn) historyModal.closeBtn.addEventListener('click', () => this.hideHistoryFilterModal());
                if (historyModal.cancelBtn) historyModal.cancelBtn.addEventListener('click', () => this.hideHistoryFilterModal());
                if (historyModal.viewBtn) historyModal.viewBtn.addEventListener('click', () => App.actions.viewHistory());
                if (historyModal.clearBtn) historyModal.clearBtn.addEventListener('click', () => App.actions.clearHistory());

                if (App.elements.monitoramentoAereo.btnHistory) {
                    // This listener is now attached in showTab
                }

                const syncHistoryModal = App.elements.syncHistoryDetailModal;
                if (syncHistoryModal.overlay) syncHistoryModal.overlay.addEventListener('click', e => { if (e.target === syncHistoryModal.overlay) this.hideSyncHistoryDetailModal(); });
                if (syncHistoryModal.closeBtn) syncHistoryModal.closeBtn.addEventListener('click', () => this.hideSyncHistoryDetailModal());
                if (syncHistoryModal.cancelBtn) syncHistoryModal.cancelBtn.addEventListener('click', () => this.hideSyncHistoryDetailModal());

                // Adiciona o event listener para o botão de retentativa dentro do modal
                if (syncHistoryModal.body) {
                    syncHistoryModal.body.addEventListener('click', e => {
                        const button = e.target.closest('button[data-action="retry-sync-item"]');
                        if (button) {
                            const { logId, itemIndex } = button.dataset;
                            App.ui.retrySyncItem(logId, parseInt(itemIndex, 10)); // CORREÇÃO: Chamada para a função no módulo correto (ui)
                        }
                    });
                }

                const syncHistoryList = document.getElementById('syncHistoryList');
                if (syncHistoryList) {
                    syncHistoryList.addEventListener('click', e => {
                        const button = e.target.closest('button[data-action="view-sync-details"]');
                        if (button) {
                            this.renderSyncHistoryDetails(button.dataset.id);
                        }
                    });
                }

                // Listeners para salvar rascunhos de formulários automaticamente
                const debouncedSaveBroca = App.debounce(() => App.actions.saveFormDraft('broca'), 1000);
                if (App.elements.broca.form) App.elements.broca.form.addEventListener('input', debouncedSaveBroca);

                const debouncedSavePerda = App.debounce(() => App.actions.saveFormDraft('perda'), 1000);
                if (App.elements.perda.form) App.elements.perda.form.addEventListener('input', debouncedSavePerda);

                const debouncedSaveCigarrinha = App.debounce(() => App.actions.saveFormDraft('cigarrinha'), 1000);
                if (App.elements.cigarrinha.form) App.elements.cigarrinha.form.addEventListener('input', debouncedSaveCigarrinha);

                const btnSaveCompanySettings = document.getElementById('btnSaveCompanySettings');
                if (btnSaveCompanySettings) {
                    // This button is now only for the logo, shapefile, etc.
                    // The calculation method will save on change.
                }

                const cigarrinhaCalcMethodSelect = document.getElementById('cigarrinhaCalcMethod');
                // O listener de 'change' foi removido e substituído por um botão explícito de salvar
                const btnSaveCalcMethod = document.getElementById('btnSaveCalcMethod');
                if (btnSaveCalcMethod) {
                    btnSaveCalcMethod.addEventListener('click', () => App.actions.saveCalcMethodWithAudit());
                }

                const btnSaveDailyPlantingGoal = document.getElementById('btnSaveDailyPlantingGoal');

                const btnViewCalcHistory = document.getElementById('btnViewCalcHistory');
                if (btnViewCalcHistory) {
                    btnViewCalcHistory.addEventListener('click', () => App.actions.viewConfigHistory());
                }
                const configModal = App.elements.configHistoryModal;
                if (configModal.overlay) configModal.overlay.addEventListener('click', e => { if (e.target === configModal.overlay) App.ui.hideConfigHistoryModal(); });
                if (configModal.closeBtn) configModal.closeBtn.addEventListener('click', () => App.ui.hideConfigHistoryModal());
                if (configModal.cancelBtn) configModal.cancelBtn.addEventListener('click', () => App.ui.hideConfigHistoryModal());

                // [NOVO] Listeners para a pesquisa no mapa
                const mapSearchBtn = App.elements.monitoramentoAereo.mapFarmSearchBtn;
                const mapSearchInput = App.elements.monitoramentoAereo.mapFarmSearchInput;
                const mapContainer = App.elements.monitoramentoAereo.mapContainer;

                if (mapSearchBtn) {
                    mapSearchBtn.addEventListener('click', (e) => {
                        e.stopPropagation();
                        App.mapModule.toggleSearch();
                    });
                }
                if (mapSearchInput) {
                    mapSearchInput.addEventListener('keypress', (e) => {
                        if (e.key === 'Enter') {
                            App.mapModule.searchFarmOnMap();
                        }
                    });
                }
                if (mapContainer) {
                    mapContainer.addEventListener('click', (e) => {
                        const searchContainer = document.querySelector('.map-search-container');
                        // Se a busca estiver ativa e o clique não foi dentro do container de busca
                        if (searchContainer.classList.contains('active') && !e.target.closest('.map-search-container')) {
                            App.mapModule.closeSearch();
                        }
                    });
                }

                // Dirty flag for Apontamento de Plantio form
                const plantioForm = App.elements.apontamentoPlantio.form;
                if (plantioForm) {
                    plantioForm.addEventListener('input', () => {
                        App.state.apontamentoPlantioFormIsDirty = true;
                    });
                }
                const btnAddPlantioRecord = App.elements.apontamentoPlantio.addRecordBtn;
                if (btnAddPlantioRecord) {
                    btnAddPlantioRecord.addEventListener('click', () => {
                        App.state.apontamentoPlantioFormIsDirty = true;
                    });
                }
                 const plantioRecordsContainer = App.elements.apontamentoPlantio.recordsContainer;
                if (plantioRecordsContainer) {
                    plantioRecordsContainer.addEventListener('click', (e) => {
                        if (e.target.closest('.btn-remover-amostra')) {
                            App.state.apontamentoPlantioFormIsDirty = true;
                        }
                    });
                }
            }
        },
        
        actions: {
            async viewConfigHistory() {
                const modal = App.elements.configHistoryModal;
                modal.body.innerHTML = '<div class="spinner-container" style="display:flex; justify-content:center; padding: 20px;"><div class="spinner"></div></div>';
                modal.overlay.classList.add('show');

                try {
                    const q = query(
                        collection(db, 'configChangeHistory'),
                        where("companyId", "==", App.state.currentUser.companyId),
                        orderBy("timestamp", "desc")
                    );
                    const querySnapshot = await getDocs(q);

                    if (querySnapshot.empty) {
                        modal.body.innerHTML = '<p style="text-align:center; padding: 20px; color: var(--color-text-light);">Nenhum histórico de alterações encontrado.</p>';
                        return;
                    }

                    let contentHTML = '';
                    querySnapshot.forEach(doc => {
                        const log = doc.data();
                        const logTimestamp = log.timestamp ? log.timestamp.toDate().toLocaleString('pt-BR') : 'Data não disponível';

                        contentHTML += `
                            <div class="plano-card" style="border-left-color: var(--color-purple);">
                                <div class="plano-header">
                                    <span class="plano-title"><i class="fas fa-user-edit"></i> Alterado por: ${log.username || 'Sistema'}</span>
                                    <span class="plano-status" style="background-color: var(--color-text-light); font-size: 12px; text-transform: none;">
                                        ${logTimestamp}
                                    </span>
                                </div>
                                <div class="plano-details" style="grid-template-columns: 1fr;">
                                    <div><strong>Alteração:</strong> ${log.alteracao}</div>
                                    <div><strong>De:</strong> ${log.valorAntigo}</div>
                                    <div><strong>Para:</strong> ${log.valorNovo}</div>
                                    <div style="margin-top: 8px;"><strong>Motivo:</strong> ${log.motivo}</div>
                                </div>
                            </div>
                        `;
                    });
                    modal.body.innerHTML = contentHTML;

                } catch (error) {
                    console.error("Erro ao carregar histórico de configurações:", error);
                    modal.body.innerHTML = '<p style="text-align:center; padding: 20px; color: var(--color-danger);">Erro ao carregar o histórico.</p>';
                }
            },

            async checkActiveConnection() {
                if (App.state.isCheckingConnection || !navigator.onLine) return;
                App.state.isCheckingConnection = true;
                console.log("Actively checking internet connection...");
                try {
                    // This is a lightweight request. A successful response (even if opaque) indicates connectivity.
                    await fetch('https://www.gstatic.com/firebasejs/9.15.0/firebase-app.js', {
                        mode: 'no-cors',
                        method: 'HEAD', // Use HEAD to be even more lightweight
                        cache: 'no-store' // Avoid hitting the browser cache
                    });

                    console.log("Active connection confirmed.");
                    // Stop the periodic check once connection is confirmed
                    if (App.state.connectionCheckInterval) {
                        clearInterval(App.state.connectionCheckInterval);
                        App.state.connectionCheckInterval = null;
                        console.log("Periodic connection check stopped.");
                    }
                    // Now, proceed with the actual synchronization logic
                    this.forceTokenRefresh(false);

                } catch (error) {
                    console.warn("Active connection check failed. Still effectively offline.");
                } finally {
                    App.state.isCheckingConnection = false;
                }
            },

            filterDashboardData(data, startDate, endDate) {
                if (!startDate || !endDate) {
                    return data;
                }
                return data.filter(item => {
                    const itemDate = item.data || item.date; // Handle both 'data' and 'date' properties
                    return itemDate >= startDate && itemDate <= endDate;
                });
            },
            saveDashboardDates(type, start, end) {
                localStorage.setItem(`dashboard-${type}-start`, start);
                localStorage.setItem(`dashboard-${type}-end`, end);
            },
            getDashboardDates(type) {
                return {
                    start: localStorage.getItem(`dashboard-${type}-start`),
                    end: localStorage.getItem(`dashboard-${type}-end`)
                };
            },
            formatDateForInput(dateString) {
                if (!dateString || typeof dateString !== 'string') return '';
                if (dateString.includes('/')) {
                    const parts = dateString.split('/');
                    if (parts.length === 3) {
                        return `${parts[2]}-${parts[1].padStart(2, '0')}-${parts[0].padStart(2, '0')}`;
                    }
                }
                const date = new Date(dateString);
                if (isNaN(date.getTime())) {
                    return '';
                }
                const offset = date.getTimezoneOffset();
                const adjustedDate = new Date(date.getTime() - (offset*60*1000));
                return adjustedDate.toISOString().split('T')[0];
            },
            formatDateForDisplay(dateString) {
                if (!dateString) return 'N/A';
                const date = new Date(dateString + 'T03:00:00Z');
                if (isNaN(date.getTime())) {
                    return 'Data Inválida';
                }
                return date.toLocaleDateString('pt-BR');
            },
            resetInactivityTimer() {
                clearTimeout(App.state.inactivityTimer);
                clearTimeout(App.state.inactivityWarningTimer);
        
                if (App.state.currentUser) {
                    App.state.inactivityWarningTimer = setTimeout(() => {
                        const { confirmationModal } = App.elements;
                        
                        confirmationModal.title.textContent = "Sessão prestes a expirar";
                        confirmationModal.message.textContent = "A sua sessão será encerrada em 1 minuto por inatividade. Deseja continuar conectado?";
                        confirmationModal.confirmBtn.textContent = "Continuar";
                        confirmationModal.cancelBtn.style.display = 'none';
        
                        const confirmHandler = () => {
                            this.resetInactivityTimer();
                            closeHandler();
                        };
        
                        const closeHandler = () => {
                            confirmationModal.overlay.classList.remove('show');
                            confirmationModal.confirmBtn.removeEventListener('click', confirmHandler);
                            confirmationModal.closeBtn.removeEventListener('click', closeHandler);
                            setTimeout(() => {
                                confirmationModal.confirmBtn.textContent = "Confirmar";
                                confirmationModal.cancelBtn.style.display = 'inline-flex';
                            }, 300);
                        };
        
                        confirmationModal.confirmBtn.addEventListener('click', confirmHandler);
                        confirmationModal.closeBtn.addEventListener('click', closeHandler);
                        confirmationModal.overlay.classList.add('show');
        
                    }, App.config.inactivityTimeout - App.config.inactivityWarningTime);
        
                    App.state.inactivityTimer = setTimeout(() => {
                        App.ui.showAlert('Sessão expirada por inatividade.', 'warning');
                        App.auth.logout();
                    }, App.config.inactivityTimeout);
                }
            },
            saveUserProfileLocally(userProfile) {
                let profiles = this.getLocalUserProfiles();
                const index = profiles.findIndex(p => p.uid === userProfile.uid);
                if (index > -1) {
                    profiles[index] = userProfile;
                } else {
                    profiles.push(userProfile);
                }
                localStorage.setItem('localUserProfiles', JSON.stringify(profiles));
            },
            getLocalUserProfiles() {
                return JSON.parse(localStorage.getItem('localUserProfiles') || '[]');
            },
            removeUserProfileLocally(userId) {
                let profiles = this.getLocalUserProfiles();
                profiles = profiles.filter(p => p.uid !== userId);
                localStorage.setItem('localUserProfiles', JSON.stringify(profiles));
            },
            async changePassword() {
                const els = App.elements.changePasswordModal;
                const currentPassword = els.currentPassword.value;
                const newPassword = els.newPassword.value;
                const confirmNewPassword = els.confirmNewPassword.value;
                
                if (!currentPassword || !newPassword || !confirmNewPassword) { App.ui.showAlert("Preencha todos os campos.", "error"); return; }
                if (newPassword !== confirmNewPassword) { App.ui.showAlert("As novas senhas não coincidem.", "error"); return; }
                if (newPassword.length < 6) { App.ui.showAlert("A nova senha deve ter pelo menos 6 caracteres.", "error"); return; }
                
                App.ui.setLoading(true, "A alterar senha...");
                try {
                    const user = auth.currentUser;
                    const credential = EmailAuthProvider.credential(user.email, currentPassword);
                    
                    await reauthenticateWithCredential(user, credential);
                    await updatePassword(user, newPassword);
                    
                    App.ui.showAlert("Senha alterada com sucesso!", "success");
                    els.overlay.classList.remove('show');
                    els.currentPassword.value = '';
                    els.newPassword.value = '';
                    els.confirmNewPassword.value = '';
                } catch (error) {
                    if (error.code === 'auth/wrong-password' || error.code === 'auth/invalid-credential') {
                        App.ui.showAlert("A senha atual está incorreta.", "error");
                    } else {
                        App.ui.showAlert("Erro ao alterar senha. Tente fazer login novamente.", "error");
                    }
                    console.error("Erro ao alterar senha:", error);
                } finally {
                    App.ui.setLoading(false);
                }
            },
            getUnavailableTalhaoIds(options = {}) {
                const { editingGroupId = null } = options;
                const unavailableIds = new Set();
                const allPlans = App.state.harvestPlans || [];

                // 1. Get plots from all saved plans.
                allPlans.forEach(plan => {
                    const closedIdsInThisPlan = new Set(plan.closedTalhaoIds || []);
                    (plan.sequence || []).forEach(group => {
                        (group.plots || []).forEach(plot => {
                            if (!closedIdsInThisPlan.has(plot.talhaoId)) {
                                unavailableIds.add(plot.talhaoId);
                            }
                        });
                    });
                });

                // 2. Add plots from the current unsaved plan's sequence in the UI.
                if (App.state.activeHarvestPlan && App.state.activeHarvestPlan.sequence) {
                    App.state.activeHarvestPlan.sequence.forEach(group => {
                        // If editing, exclude the group being edited so its plots can be re-selected.
                        if (editingGroupId && group.id == editingGroupId) {
                            return;
                        }
                        (group.plots || []).forEach(plot => {
                            unavailableIds.add(plot.talhaoId);
                        });
                    });
                }

                return unavailableIds;
            },
            async saveFarm() {
                const { farmCode, farmName, farmTypeCheckboxes } = App.elements.cadastros;
                const code = farmCode.value.trim();
                const name = farmName.value.trim().toUpperCase();
                const types = Array.from(farmTypeCheckboxes).filter(cb => cb.checked).map(cb => cb.value);

                if (!code || !name) { App.ui.showAlert("Código e Nome da fazenda são obrigatórios.", "error"); return; }
                
                const existingFarm = App.state.fazendas.find(f => f.code === code);
                if (existingFarm) {
                    App.ui.showAlert("Já existe uma fazenda com este código.", "error");
                    return;
                }

                App.ui.showConfirmationModal(`Tem a certeza que deseja guardar a fazenda ${name}?`, async () => {
                    let targetCompanyId = App.state.currentUser.companyId;
                    if (App.state.currentUser.role === 'super-admin') {
                        targetCompanyId = App.elements.cadastros.adminTargetCompanyFarms.value;
                        if (!targetCompanyId) {
                            App.ui.showAlert("Como Super Admin, você deve selecionar uma empresa alvo para criar a fazenda.", "error");
                            return;
                        }
                    }
                    try {
                        await App.data.addDocument('fazendas', { code, name, types, talhoes: [], companyId: targetCompanyId });
                        App.ui.showAlert("Fazenda adicionada com sucesso!");
                        farmCode.value = ''; 
                        farmName.value = '';
                        farmTypeCheckboxes.forEach(cb => cb.checked = false);
                    } catch (error) {
                        App.ui.showAlert("Erro ao guardar fazenda.", "error");
                    }
                });
            },
            async saveFarmChanges() {
                const modal = App.elements.editFarmModal;
                const farmId = modal.editingFarmId.value;
                const newName = modal.nameInput.value.trim().toUpperCase();
                const newTypes = Array.from(modal.typeCheckboxes).filter(cb => cb.checked).map(cb => cb.value);

                if (!newName) {
                    App.ui.showAlert("O nome da fazenda não pode ficar em branco.", "error");
                    return;
                }

                try {
                    await App.data.updateDocument('fazendas', farmId, { name: newName, types: newTypes });
                    App.ui.showAlert("Dados da fazenda atualizados com sucesso!");
                    App.ui.closeEditFarmModal();
                } catch (error) {
                    App.ui.showAlert("Erro ao atualizar os dados da fazenda.", "error");
                    console.error(error);
                }
            },
            deleteFarm(farmId) {
                const farm = App.state.fazendas.find(f => f.id === farmId);
                if (!farm) return;
                
                App.ui.showConfirmationModal(`Tem a certeza que deseja excluir a fazenda "${farm.name}" e todos os seus talhões? Esta ação é irreversível.`, async () => {
                    try {
                        await App.data.deleteDocument('fazendas', farmId);
                        App.ui.showAlert('Fazenda excluída com sucesso.', 'info');
                        App.elements.cadastros.farmSelect.value = '';
                        App.elements.cadastros.talhaoManagementContainer.style.display = 'none';
                    } catch (error) {
                        App.ui.showAlert('Erro ao excluir a fazenda.', 'error');
                        console.error(error);
                    }
                });
            },
            deleteAllFarms() {
                App.ui.showConfirmationModal("ATENÇÃO! Você está prestes a excluir TODAS as fazendas e talhões cadastrados. Esta ação é IRREVERSÍVEL. Digite 'EXCLUIR TUDO' para confirmar.", async (confirmationInput) => {
                    if (confirmationInput !== 'EXCLUIR TUDO') {
                        App.ui.showAlert("A confirmação não corresponde. Ação cancelada.", "warning");
                        return;
                    }
                    
                    App.ui.setLoading(true, "Excluindo todas as fazendas...");
                    try {
                        const batch = writeBatch(db);
                        App.state.fazendas.forEach(farm => {
                            const docRef = doc(db, 'fazendas', farm.id);
                            batch.delete(docRef);
                        });
                        await batch.commit();
                        App.ui.showAlert('Todas as fazendas foram excluídas com sucesso.', 'success');
                    } catch (error) {
                        App.ui.showAlert('Erro ao excluir todas as fazendas.', 'error');
                        console.error(error);
                    } finally {
                        App.ui.setLoading(false);
                    }
                }, true);
            },
            calculateTalhaoProducao() {
                const { talhaoArea, talhaoTCH, talhaoProducao } = App.elements.cadastros;
                const area = parseFloat(talhaoArea.value) || 0;
                const tch = parseFloat(talhaoTCH.value) || 0;
                talhaoProducao.value = (area * tch).toFixed(2);
            },
            async saveTalhao() {
                const { farmSelect, talhaoId, talhaoName, talhaoArea, talhaoTCH, talhaoProducao, talhaoCorte, talhaoVariedade, talhaoDistancia, talhaoUltimaColheita } = App.elements.cadastros;
                const farmId = farmSelect.value;
                if (!farmId) { App.ui.showAlert("Selecione uma fazenda.", "error"); return; }
                
                const farm = App.state.fazendas.find(f => f.id === farmId);
                if (!farm) { App.ui.showAlert("Fazenda selecionada não encontrada.", "error"); return; }
                
                const talhaoData = {
                    id: talhaoId.value ? parseInt(talhaoId.value) : Date.now(),
                    name: talhaoName.value.trim().toUpperCase(),
                    area: parseFloat(talhaoArea.value) || 0,
                    tch: parseFloat(talhaoTCH.value) || 0,
                    producao: parseFloat(talhaoProducao.value) || 0,
                    corte: parseInt(talhaoCorte.value) || 1,
                    variedade: talhaoVariedade.value.trim(),
                    distancia: parseFloat(talhaoDistancia.value) || 0,
                    dataUltimaColheita: this.formatDateForInput(talhaoUltimaColheita.value)
                };
                if (!talhaoData.name || isNaN(talhaoData.area) || isNaN(talhaoData.tch)) { App.ui.showAlert("Nome, Área e TCH do talhão são obrigatórios.", "error"); return; }
                
                App.ui.showConfirmationModal(`Tem a certeza que deseja guardar o talhão ${talhaoData.name}?`, async () => {
                    let updatedTalhoes = farm.talhoes ? [...farm.talhoes] : [];
                    const existingIndex = updatedTalhoes.findIndex(t => t.id === talhaoData.id);

                    if (existingIndex > -1) {
                        updatedTalhoes[existingIndex] = talhaoData;
                    } else {
                        updatedTalhoes.push(talhaoData);
                    }
                    
                    try {
                        await App.data.updateDocument('fazendas', farm.id, { talhoes: updatedTalhoes });
                        App.ui.showAlert("Talhão guardado com sucesso!");
                        [talhaoId, talhaoName, talhaoArea, talhaoTCH, talhaoProducao, talhaoCorte, talhaoVariedade, talhaoDistancia, talhaoUltimaColheita].forEach(el => el.value = '');
                        App.elements.cadastros.talhaoName.focus();
                    } catch(error) {
                        App.ui.showAlert("Erro ao guardar talhão.", "error");
                        console.error("Erro ao guardar talhão:", error);
                    }
                });
            },
            editTalhao(talhaoId) {
                const { farmSelect, ...talhaoEls } = App.elements.cadastros;
                const farm = App.state.fazendas.find(f => f.id === farmSelect.value);
                const talhao = farm?.talhoes.find(t => t.id == talhaoId);
                if (talhao) {
                    talhaoEls.talhaoId.value = talhao.id;
                    talhaoEls.talhaoName.value = talhao.name;
                    talhaoEls.talhaoArea.value = talhao.area;
                    talhaoEls.talhaoTCH.value = talhao.tch;
                    talhaoEls.talhaoProducao.value = talhao.producao;
                    talhaoEls.talhaoCorte.value = talhao.corte;
                    talhaoEls.talhaoVariedade.value = talhao.variedade;
                    talhaoEls.talhaoDistancia.value = talhao.distancia;
                    talhaoEls.talhaoUltimaColheita.value = this.formatDateForInput(talhao.dataUltimaColheita);
                    talhaoEls.talhaoName.focus();
                }
            },
            async deleteTalhao(talhaoId) {
                const farm = App.state.fazendas.find(f => f.id === App.elements.cadastros.farmSelect.value);
                if (farm && farm.talhoes) {
                    App.ui.showConfirmationModal("Tem a certeza que deseja excluir este talhão?", async () => {
                        const updatedTalhoes = farm.talhoes.filter(t => t.id != talhaoId);
                        try {
                            await App.data.updateDocument('fazendas', farm.id, { talhoes: updatedTalhoes });
                            App.ui.showAlert('Talhão excluído com sucesso.', 'info');
                        } catch(e) {
                            App.ui.showAlert('Erro ao excluir talhão.', 'error');
                        }
                    });
                }
            },
            async saveFrenteDePlantio() {
                const { id, name, provider, obs } = App.elements.frenteDePlantio;
                const nameValue = name.value.trim();
                const providerValue = provider.value.trim();
                const obsValue = obs.value.trim();
                if (!nameValue || !providerValue) {
                    App.ui.showAlert("O Nome da Frente e o Prestador Vinculado são obrigatórios.", "error");
                    return;
                }

                const existingId = id.value;
                const data = {
                    name: nameValue,
                    provider: providerValue,
                    obs: obsValue,
                    companyId: App.state.currentUser.companyId
                };

                const confirmationMessage = existingId
                    ? `Tem a certeza que deseja atualizar a frente de plantio "${nameValue}"?`
                    : `Tem a certeza que deseja guardar a nova frente de plantio "${nameValue}"?`;

                App.ui.showConfirmationModal(confirmationMessage, async () => {
                    App.ui.setLoading(true, "A guardar...");
                    try {
                        if (existingId) {
                            if (!navigator.onLine) {
                                App.ui.showAlert("A edição não está disponível offline. Conecte-se para atualizar.", "warning");
                                return;
                            }
                            await App.data.updateDocument('frentesDePlantio', existingId, data);
                            App.ui.showAlert("Frente de Plantio atualizada com sucesso!");
                        } else {
                            await App.data.addDocument('frentesDePlantio', data);
                            App.ui.showAlert("Frente de Plantio guardada com sucesso!");
                        }
                        id.value = '';
                        name.value = '';
                        provider.value = '';
                        obs.value = '';
                    } catch (error) {
                        console.error("Erro ao guardar Frente de Plantio:", error);
                        const errorMessage = `Erro ao guardar Frente de Plantio: ${error.message}.`;

                        // Apenas tenta guardar offline se for uma nova entrada e se o erro for de rede
                        if (!existingId && !navigator.onLine) {
                            try {
                                const entryId = `offline_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
                                await OfflineDB.add('offline-writes', { id: entryId, collection: 'frentesDePlantio', data: data });
                                App.ui.showAlert('Guardado offline. Será enviado quando houver conexão.', 'info');
                                id.value = ''; name.value = ''; provider.value = ''; obs.value = '';
                            } catch (offlineError) {
                                App.ui.showAlert("Falha crítica ao guardar offline.", "error");
                                console.error("Erro ao guardar offline:", offlineError);
                            }
                        } else {
                            App.ui.showAlert(errorMessage, "error");
                        }
                    } finally {
                        App.ui.setLoading(false);
                    }
                });
            },

            editFrenteDePlantio(frenteId) {
                const { id, name, provider, obs } = App.elements.frenteDePlantio;
                const frente = App.state.frentesDePlantio.find(f => f.id == frenteId);
                if (frente) {
                    id.value = frente.id;
                    name.value = frente.name;
                    provider.value = frente.provider;
                    obs.value = frente.obs;
                    name.focus();
                }
            },

            deleteFrenteDePlantio(frenteId) {
                App.ui.showConfirmationModal("Are you sure you want to delete this Frente de Plantio?", async () => {
                    await App.data.deleteDocument('frentesDePlantio', frenteId);
                    App.ui.showAlert('Frente de Plantio deleted successfully.', 'info');
                });
            },

            async saveApontamentoOffline(newEntry, els) {
                try {
                    const entryId = `offline_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
                    await OfflineDB.add('offline-writes', { id: entryId, collection: 'apontamentosPlantio', data: newEntry });
                    App.ui.showAlert('Guardado offline. Será enviado quando houver conexão.', 'info');
                    App.ui.clearForm(els.form);
                    els.recordsContainer.innerHTML = '';
                    App.ui.setDefaultDatesForEntryForms();
                    App.ui.calculateTotalPlantedArea();
                } catch (offlineError) {
                    App.ui.showAlert("Falha crítica ao guardar offline.", "error");
                    console.error("Erro ao guardar offline:", offlineError);
                }
            },

            async saveApontamentoPlantio() {
                const els = App.elements.apontamentoPlantio;
                if (!App.ui.validateFields([els.frente.id, els.leaderId.id, els.farmName.id, els.date.id])) {
                    App.ui.showAlert("Preencha todos os campos principais (Frente, Matrícula, Fazenda, Data)!", "error");
                    return;
                }

                const recordCards = els.recordsContainer.querySelectorAll('.amostra-card');
                if (recordCards.length === 0) {
                    App.ui.showAlert("Adicione pelo menos um lançamento de plantio.", "error");
                    return;
                }

                const recordsData = [];
                for (const card of recordCards) {
                    const talhaoInput = card.querySelector('input[id^="plantioTalhao-"]');
                    const talhaoSelect = card.querySelector('.plantio-talhao-select');
                    const variedadeInput = card.querySelector('input[id^="plantioVariedade-"]');
                    const areaInput = card.querySelector('input[id^="plantioArea-"]');
                    const area = parseFloat(areaInput.value) || 0;
                    const maxArea = parseFloat(areaInput.max);

                    if (!talhaoSelect.value || !variedadeInput.value || !areaInput.value) {
                        App.ui.showAlert("Preencha todos os campos em todos os lançamentos (Talhão, Variedade, Área).", "error");
                        talhaoSelect.focus();
                        return;
                    }

                    if (area > maxArea) {
                        App.ui.showAlert(`A área plantada (${area}ha) não pode exceder a área restante (${maxArea}ha) para o talhão selecionado.`, "error");
                        areaInput.focus();
                        return;
                    }

                    recordsData.push({
                        talhaoId: talhaoSelect.value,
                        talhao: talhaoSelect.options[talhaoSelect.selectedIndex].text,
                        variedade: variedadeInput.value,
                        area: area
                    });
                }

                const frente = App.state.frentesDePlantio.find(f => f.id === els.frente.value);
                const farm = App.state.fazendas.find(f => f.id === els.farmName.value);

                const newEntry = {
                    frenteDePlantioId: frente.id,
                    frenteDePlantioName: frente.name,
                    provider: els.provider.value,
                    culture: els.culture.value,
                    leaderId: els.leaderId.value,
                    farmName: farm.name,
                    farmCode: farm.code,
                    date: els.date.value,
                    chuva: els.chuva ? els.chuva.value : '',
                    obs: els.obs ? els.obs.value : '',
                    records: recordsData,
                    totalArea: recordsData.reduce((sum, rec) => sum + rec.area, 0),
                    usuario: App.state.currentUser.username,
                    companyId: App.state.currentUser.companyId
                };

                const entryId = els.entryId.value;
                const confirmationMessage = entryId ? "Tem a certeza que deseja atualizar este apontamento?" : "Tem a certeza que deseja guardar este apontamento?";

                App.ui.showConfirmationModal(confirmationMessage, async () => {
                    App.ui.setLoading(true, "A guardar...");
                    try {
                        if (navigator.onLine) {
                            if (entryId) {
                                await App.data.updateDocument('apontamentosPlantio', entryId, newEntry);
                                App.ui.showAlert("Apontamento de plantio atualizado com sucesso!");
                            } else {
                                await App.data.addDocument('apontamentosPlantio', newEntry);
                                App.ui.showAlert("Apontamento de plantio guardado com sucesso!");
                            }
                        } else {
                            if (entryId) {
                                App.ui.showAlert("A edição não está disponível offline.", "warning");
                                return;
                            }
                            await this.saveApontamentoOffline(newEntry, els);
                        }

                        App.ui.clearForm(els.form);
                        els.recordsContainer.innerHTML = '';
                        App.ui.setDefaultDatesForEntryForms();
                        App.ui.calculateTotalPlantedArea();
                        els.entryId.value = '';
                        App.state.apontamentoPlantioFormIsDirty = false;

                    } catch (error) {
                        App.ui.showAlert(`Erro ao guardar: ${error.message}.`, "error");
                        // Se a operação online falhar, tenta guardar offline
                        if (!entryId) {
                            await this.saveApontamentoOffline(newEntry, els);
                        }
                    } finally {
                        App.ui.setLoading(false);
                    }
                });
            },

            async saveLancamentoClima() {
                const els = App.elements.lancamentoClima;
                const requiredFields = ['climaData', 'climaFazenda', 'climaTalhao', 'climaTempMax', 'climaTempMin', 'climaUmidade', 'climaPluviosidade', 'climaVento'];

                if (!App.ui.validateFields(requiredFields)) {
                    App.ui.showAlert("Preencha todos os campos obrigatórios!", "error");
                    return;
                }

                const farm = App.state.fazendas.find(f => f.id === els.fazenda.value);
                if (!farm) {
                    App.ui.showAlert("Fazenda não encontrada.", "error");
                    return;
                }

                const newEntry = {
                    data: els.data.value,
                    fazendaId: farm.id,
                    fazendaNome: farm.name,
                    talhaoNome: els.talhao.value,
                    tempMax: parseFloat(els.tempMax.value),
                    tempMin: parseFloat(els.tempMin.value),
                    umidade: parseFloat(els.umidade.value),
                    pluviosidade: parseFloat(els.pluviosidade.value),
                    vento: parseFloat(els.vento.value),
                    obs: els.obs.value,
                    usuario: App.state.currentUser.username,
                    companyId: App.state.currentUser.companyId
                };

                const entryId = els.entryId.value;
                const confirmationMessage = entryId ? "Tem a certeza que deseja atualizar este apontamento climatológico?" : "Tem a certeza que deseja guardar este apontamento climatológico?";

                App.ui.showConfirmationModal(confirmationMessage, async () => {
                    App.ui.setLoading(true, "A guardar...");
                    try {
                        if (navigator.onLine) {
                            if (entryId) {
                                await App.data.updateDocument('clima', entryId, newEntry);
                                App.ui.showAlert("Apontamento atualizado com sucesso!");
                            } else {
                                await App.data.addDocument('clima', newEntry);
                                App.ui.showAlert("Apontamento guardado com sucesso!");
                            }
                        } else {
                            if (entryId) {
                                App.ui.showAlert("A edição não está disponível offline.", "warning");
                                return;
                            }
                            const offlineId = `offline_clima_${Date.now()}`;
                            await OfflineDB.add('offline-writes', { id: offlineId, collection: 'clima', data: newEntry });
                            App.ui.showAlert('Guardado offline. Será sincronizado quando houver conexão.', 'info');
                        }
                        App.ui.clearForm(els.form);
                        els.entryId.value = '';
                        App.ui.setDefaultDatesForEntryForms();
                    } catch (error) {
                        App.ui.showAlert(`Erro ao guardar: ${error.message}.`, "error");
                        if (!entryId) {
                            try {
                                const offlineId = `offline_clima_${Date.now()}`;
                                await OfflineDB.add('offline-writes', { id: offlineId, collection: 'clima', data: newEntry });
                                App.ui.showAlert('Falha ao conectar. Apontamento guardado offline.', 'warning');
                                App.ui.clearForm(els.form);
                                els.entryId.value = '';
                                App.ui.setDefaultDatesForEntryForms();
                            } catch (offlineError) {
                                App.ui.showAlert("Falha crítica ao guardar offline.", "error");
                            }
                        }
                    } finally {
                        App.ui.setLoading(false);
                    }
                });
            },


            async savePersonnel() {
                const { id, matricula, name } = App.elements.personnel;
                const matriculaValue = matricula.value.trim();
                const nameValue = name.value.trim();
                if (!matriculaValue || !nameValue) { App.ui.showAlert("Matrícula e Nome são obrigatórios.", "error"); return; }
                
                const existingId = id.value;
                const data = { matricula: matriculaValue, name: nameValue, companyId: App.state.currentUser.companyId };
                
                App.ui.showConfirmationModal(`Tem a certeza que deseja guardar os dados de ${nameValue}?`, async () => {
                    try {
                        if (existingId) {
                            await App.data.updateDocument('personnel', existingId, data);
                        } else {
                            await App.data.addDocument('personnel', data);
                        }
                        App.ui.showAlert("Pessoa guardada com sucesso!");
                        id.value = ''; matricula.value = ''; name.value = '';
                    } catch (e) {
                        App.ui.showAlert("Erro ao guardar pessoa.", "error");
                    }
                });
            },
            editPersonnel(personnelId) {
                const { id, matricula, name } = App.elements.personnel;
                const person = App.state.personnel.find(p => p.id == personnelId);
                if (person) {
                    id.value = person.id;
                    matricula.value = person.matricula;
                    name.value = person.name;
                    matricula.focus();
                }
            },
            deletePersonnel(personnelId) {
                App.ui.showConfirmationModal("Tem certeza que deseja excluir esta pessoa?", async () => {
                    await App.data.deleteDocument('personnel', personnelId);
                    App.ui.showAlert('Pessoa excluída com sucesso.', 'info');
                });
            },
            async handleLogoUpload(e) {
                const file = e.target.files[0];
                const input = e.target;
                if (!file) return;

                if (!file.type.startsWith('image/')) {
                    App.ui.showAlert('Por favor, selecione um ficheiro de imagem (PNG, JPG, etc.).', 'error');
                    input.value = '';
                    return;
                }

                const MAX_SIZE_MB = 1;
                if (file.size > MAX_SIZE_MB * 1024 * 1024) {
                    App.ui.showAlert(`O ficheiro é muito grande. O tamanho máximo é de ${MAX_SIZE_MB}MB para armazenamento direto.`, 'error');
                    input.value = '';
                    return;
                }

                App.ui.setLoading(true, "A carregar logo...");

                const reader = new FileReader();
                reader.onload = async (event) => {
                    const base64String = event.target.result;
                    try {
                        await App.data.setDocument('config', App.state.currentUser.companyId, { logoBase64: base64String }, { merge: true });
                        App.ui.showAlert('Logo carregado com sucesso!');
                    } catch (error) {
                        console.error("Erro ao carregar o logo para o Firestore:", error);
                        App.ui.showAlert(`Erro ao carregar o logo: ${error.message}`, 'error');
                    } finally {
                        App.ui.setLoading(false);
                        input.value = '';
                    }
                };
                reader.onerror = (error) => {
                    App.ui.setLoading(false);
                    App.ui.showAlert('Erro ao ler o ficheiro.', 'error');
                    console.error("Erro FileReader:", error);
                };
                reader.readAsDataURL(file);
            },
            removeLogo() {
                App.ui.showConfirmationModal("Tem a certeza que deseja remover o logotipo?", async () => {
                    App.ui.setLoading(true, "A remover logo...");
                    try {
                        await App.data.updateDocument('config', App.state.currentUser.companyId, { logoBase64: null });
                        App.ui.showAlert('Logo removido com sucesso!');
                    } catch (error) {
                        console.error("Erro ao remover logo do Firestore:", error);
                        App.ui.showAlert(`Erro ao remover o logo: ${error.message}`, 'error');
                    } finally {
                        App.ui.setLoading(false);
                        App.elements.companyConfig.logoInput.value = '';
                    }
                });
            },

            async saveCalcMethodWithAudit() {
                const companyId = App.state.currentUser.companyId;
                if (!companyId) return;

                const selectEl = document.getElementById('cigarrinhaCalcMethod');
                const newValue = selectEl.value;
                const oldValue = App.state.companyConfig?.cigarrinhaCalcMethod || '5';

                if (newValue === oldValue) {
                    App.ui.showAlert("Nenhuma alteração detectada.", "info");
                    return;
                }

                App.ui.showConfirmationModal(
                    `Tem a certeza que deseja alterar o método de cálculo de ÷${oldValue} para ÷${newValue}? Por favor, forneça um motivo para esta alteração.`,
                    async (input) => {
                        const motivo = input.motivoAlteracao;
                        if (!motivo || motivo.trim() === '') {
                            App.ui.showAlert("O motivo é obrigatório para registar a alteração.", "error");
                            return;
                        }

                        App.ui.setLoading(true, "A guardar alteração e a registar auditoria...");

                        const logEntry = {
                            companyId: companyId,
                            userId: App.state.currentUser.uid,
                            username: App.state.currentUser.username,
                            timestamp: serverTimestamp(),
                            alteracao: `Método de cálculo de cigarrinha (amostragem)`,
                            valorAntigo: oldValue,
                            valorNovo: newValue,
                            motivo: motivo.trim()
                        };

                        try {
                            const batch = writeBatch(db);
                            const configRef = doc(db, 'config', companyId);
                            batch.set(configRef, { cigarrinhaCalcMethod: newValue }, { merge: true });
                            const logRef = doc(collection(db, 'configChangeHistory'));
                            batch.set(logRef, logEntry);
                            await batch.commit();

                            App.state.companyConfig.cigarrinhaCalcMethod = newValue;
                            App.ui.showAlert("Método de cálculo alterado e auditoria registada com sucesso!", "success");

                        } catch (error) {
                            console.error("Erro ao guardar método de cálculo com auditoria:", error);
                            App.ui.showAlert("Ocorreu um erro ao guardar a alteração.", "error");
                        } finally {
                            App.ui.setLoading(false);
                        }
                    },
                    [{
                        type: 'textarea',
                        id: 'motivoAlteracao',
                        placeholder: 'Ex: Ajuste para o padrão da nova safra.',
                        required: true
                    }]
                );
            },

            async agendarInspecao() {
                const els = App.elements.planejamento;
                const farmId = els.fazenda.value;
                const farm = App.state.fazendas.find(f => f.id === farmId);
                if (!farm) { App.ui.showAlert("Fazenda inválida.", "error"); return; }

                const campos = { tipo: els.tipo.value, fazendaCodigo: farm.code, talhao: els.talhao.value.trim(), dataPrevista: els.data.value, usuarioResponsavel: els.responsavel.value };
                if (Object.values(campos).some(v => !v)) { App.ui.showAlert("Todos os campos obrigatórios devem ser preenchidos.", "error"); return; }
                
                App.ui.showConfirmationModal("Tem a certeza que deseja agendar esta inspeção?", async () => {
                    const novoPlano = { ...campos, meta: els.meta.value || null, observacoes: els.obs.value.trim() || null, status: 'Pendente', companyId: App.state.currentUser.companyId };
                    await App.data.addDocument('planos', novoPlano);
                    App.ui.showAlert("Inspeção agendada com sucesso!");
                    els.talhao.value = ''; els.data.value = ''; els.meta.value = ''; els.obs.value = '';
                });
            },
            async marcarPlanoComoConcluido(id) {
                App.ui.showConfirmationModal("Marcar esta inspeção como concluída?", async () => {
                    await App.data.updateDocument('planos', id, { status: 'Concluído' });
                    App.ui.showAlert("Inspeção marcada como concluída!", "success");
                });
            },
            excluirPlano(id) {
                App.ui.showConfirmationModal("Tem a certeza que deseja excluir este planejamento?", async () => {
                    await App.data.deleteDocument('planos', id);
                    App.ui.showAlert("Planejamento excluído.", "info");
                });
            },
            async verificarEAtualizarPlano(tipo, fazendaCodigo, talhao) {
                const planoPendente = App.state.planos.find(p => p.status === 'Pendente' && p.tipo === tipo && p.fazendaCodigo === fazendaCodigo && p.talhao.toLowerCase() === talhao.toLowerCase());
                if (planoPendente) {
                    await this.marcarPlanoComoConcluido(planoPendente.id);
                    App.ui.showAlert(`Planejamento correspondente para ${talhao} foi concluído automaticamente.`, 'info');
                }
            },
            findVarietyForTalhao(section) {
                const formElements = App.elements[section];
                const farmId = formElements.codigo.value;
                const talhaoName = formElements.talhao.value.trim().toUpperCase();
                const display = formElements.varietyDisplay;
                
                display.textContent = '';
                if (!farmId || !talhaoName) return;

                const farm = App.state.fazendas.find(f => f.id === farmId);
                const talhao = farm?.talhoes.find(t => t.name.toUpperCase() === talhaoName);

                if (talhao && talhao.variedade) {
                    display.textContent = `Variedade: ${talhao.variedade}`;
                }
            },
            findOperatorName() {
                const { matricula, operadorNome } = App.elements.perda;
                const matriculaValue = matricula.value.trim();
                operadorNome.textContent = '';
                if (!matriculaValue) return;

                const operator = App.state.personnel.find(p => p.matricula === matriculaValue);
                if (operator) {
                    operadorNome.textContent = operator.name;
                    operadorNome.style.color = 'var(--color-primary)';
                } else {
                    operadorNome.textContent = 'Operador não encontrado';
                    operadorNome.style.color = 'var(--color-danger)';
                }
            },

            findLeaderName() {
                const { leaderId, leaderName } = App.elements.apontamentoPlantio;
                const matriculaValue = leaderId.value.trim();
                leaderName.textContent = '';
                if (!matriculaValue) return;

                const leader = App.state.personnel.find(p => p.matricula === matriculaValue);
                if (leader) {
                    leaderName.textContent = leader.name;
                    leaderName.style.color = 'var(--color-primary)';
                } else {
                    leaderName.textContent = 'Líder não encontrado';
                    leaderName.style.color = 'var(--color-danger)';
                }
            },
            async createCompany() {
                const { companyName, adminEmail, adminPassword } = App.elements.companyManagement;
                const name = companyName.value.trim();
                const email = adminEmail.value.trim();
                const password = adminPassword.value.trim();

                if (!name || !email || !password) {
                    App.ui.showAlert("Todos os campos são obrigatórios.", "error");
                    return;
                }
                if (password.length < 6) {
                    App.ui.showAlert("A senha deve ter pelo menos 6 caracteres.", "error");
                    return;
                }

                const subscribedModules = Array.from(document.querySelectorAll('#newCompanyModules input:checked'))
                                               .map(cb => cb.dataset.module);

                if (subscribedModules.length === 0) {
                    App.ui.showAlert("Selecione pelo menos um módulo para a empresa.", "error");
                    return;
                }

                App.ui.setLoading(true, "A criar nova empresa...");

                let companyId = null;
                try {
                    // 1. Criar a empresa com os módulos subscritos
                    const companyRef = await App.data.addDocument('companies', {
                        name: name,
                        active: true,
                        createdAt: serverTimestamp(),
                        subscribedModules: subscribedModules
                    });
                    companyId = companyRef.id;

                    // 2. Criar o utilizador de autenticação
                    const userCredential = await createUserWithEmailAndPassword(secondaryAuth, email, password);
                    const newUser = userCredential.user;
                    await signOut(secondaryAuth);

                    // 3. Criar o documento do utilizador no Firestore
                    try {
                        const adminPermissions = App.config.roles['admin'];
                        const userData = {
                            username: email.split('@')[0],
                            email: email,
                            role: 'admin',
                            active: true,
                            permissions: adminPermissions,
                            companyId: companyId
                        };
                        await App.data.createUserData(newUser.uid, userData);

                        App.ui.showAlert(`Empresa "${name}" e administrador criados com sucesso!`);
                        companyName.value = '';
                        adminEmail.value = '';
                        adminPassword.value = '';
                        document.querySelectorAll('#newCompanyModules input').forEach(cb => cb.checked = true);

                    } catch (dbError) {
                        console.error("ERRO CRÍTICO: Falha ao gravar o utilizador no Firestore. A reverter a criação da empresa.", dbError);
                        await App.data.deleteDocument('companies', companyId);
                        const userMessage = `ERRO GRAVE: Ocorreu um erro ao guardar os dados do administrador na base de dados. A criação da empresa foi cancelada.\n\n` +
                                            `Ação necessária: O utilizador de autenticação para "${email}" foi criado mas está órfão. Por favor, ` +
                                            `elimine este utilizador manualmente na consola do Firebase (Authentication -> Users) antes de tentar novamente.\n\n` +
                                            `Detalhes do erro: ${dbError.message}`;
                        App.ui.showAlert(userMessage, "error", 15000);
                    }

                } catch (error) {
                    if (companyId) {
                        await App.data.deleteDocument('companies', companyId);
                    }

                    if (error.code === 'auth/email-already-in-use') {
                        App.ui.showAlert("Este e-mail já está em uso por outro utilizador.", "error");
                    } else if (error.code === 'auth/weak-password') {
                        App.ui.showAlert("A senha deve ter pelo menos 6 caracteres.", "error");
                    } else {
                        App.ui.showAlert("Erro ao criar a empresa ou o utilizador de autenticação.", "error");
                        console.error("Erro na criação inicial:", error);
                    }
                } finally {
                    App.ui.setLoading(false);
                }
            },

            async toggleCompanyStatus(companyId) {
                const company = App.state.companies.find(c => c.id === companyId);
                if (!company) {
                    App.ui.showAlert("Empresa não encontrada.", "error");
                    return;
                }

                const newStatus = company.active === false; // Se for explicitamente falso, o novo status é verdadeiro. Caso contrário, torna-se falso.
                const actionText = newStatus ? "ativar" : "desativar";

                App.ui.showConfirmationModal(`Tem a certeza que deseja ${actionText} a empresa "${company.name}"?`, async () => {
                    try {
                        await App.data.updateDocument('companies', companyId, { active: newStatus });
                        App.ui.showAlert(`Empresa ${newStatus ? 'ativada' : 'desativada'} com sucesso!`, 'success');
                    } catch (error) {
                        App.ui.showAlert(`Erro ao ${actionText} a empresa.`, "error");
                        console.error(`Erro ao mudar status da empresa ${companyId}:`, error);
                    }
                });
            },

            async deleteCompanyPermanently(companyId) {
                const company = App.state.companies.find(c => c.id === companyId);
                if (!company) {
                    App.ui.showAlert("Empresa não encontrada.", "error");
                    return;
                }

                const confirmationMessage = `AÇÃO IRREVERSÍVEL!\nIsto irá apagar permanentemente a empresa "${company.name}" e TODOS os seus dados associados (utilizadores, fazendas, lançamentos, etc.).\n\nPara confirmar, digite o nome exato da empresa no campo abaixo.`;

                App.ui.showConfirmationModal(
                    confirmationMessage,
                    async (userInput) => {
                        if (userInput.confirmationModalInput !== company.name) {
                            App.ui.showAlert("O nome da empresa não corresponde. A exclusão foi cancelada.", "warning");
                            return;
                        }
                        await this._executeCascadeDelete(companyId);
                    },
                    [{ id: 'confirmationModalInput', placeholder: `Digite "${company.name}"`, required: true }]
                );
            },

            async _deleteCollectionByCompanyId(collectionName, companyId, batchSize = 400) {
                const querySnapshot = await getDocs(query(collection(db, collectionName), where("companyId", "==", companyId)));
                if (querySnapshot.empty) {
                    console.log(`Nenhum documento para excluir em '${collectionName}' para a empresa ${companyId}.`);
                    return 0;
                }

                const chunks = [];
                for (let i = 0; i < querySnapshot.docs.length; i += batchSize) {
                    chunks.push(querySnapshot.docs.slice(i, i + batchSize));
                }

                let deletedCount = 0;
                for (const chunk of chunks) {
                    const batch = writeBatch(db);
                    chunk.forEach(doc => {
                        batch.delete(doc.ref);
                    });
                    await batch.commit();
                    deletedCount += chunk.length;
                }
                return deletedCount;
            },

            async saveCompanyModuleChanges() {
                const modal = App.elements.editCompanyModal;
                const companyId = modal.editingCompanyId.value;
                if (!companyId) {
                    App.ui.showAlert("ID da empresa não encontrado.", "error");
                    return;
                }

                const newSubscribedModules = Array.from(modal.modulesGrid.querySelectorAll('input:checked'))
                                                  .map(cb => cb.dataset.module);

                if (newSubscribedModules.length === 0) {
                    App.ui.showAlert("Uma empresa deve ter pelo menos um módulo subscrito.", "error");
                    return;
                }

                try {
                    await App.data.updateDocument('companies', companyId, {
                        subscribedModules: newSubscribedModules
                    });
                    App.ui.showAlert("Módulos da empresa atualizados com sucesso!", "success");
                    App.ui.closeEditCompanyModal();
                } catch (error) {
                    App.ui.showAlert("Erro ao guardar as alterações.", "error");
                    console.error("Erro ao atualizar módulos da empresa:", error);
                }
            },

            async _executeCascadeDelete(companyId) {
                App.ui.setLoading(true, "A excluir dados da empresa...");
                const collectionsToDelete = ['users', 'fazendas', 'personnel', 'registros', 'perdas', 'cigarrinha', 'planos', 'harvestPlans', 'armadilhas'];
                const errors = [];
                let totalDeleted = 0;

                for (const collectionName of collectionsToDelete) {
                    try {
                        App.ui.setLoading(true, `A excluir ${collectionName}...`);
                        const count = await this._deleteCollectionByCompanyId(collectionName, companyId);
                        totalDeleted += count;
                        console.log(`${count} documentos excluídos de ${collectionName}.`);
                    } catch (error) {
                        console.error(`Erro ao excluir a coleção ${collectionName}:`, error);
                        errors.push(collectionName);
                    }
                }

                try {
                    // Excluir o documento de configuração da empresa
                    App.ui.setLoading(true, `A excluir configurações...`);
                    await App.data.deleteDocument('config', companyId);

                    // Excluir o documento da própria empresa
                    App.ui.setLoading(true, `A finalizar exclusão...`);
                    await App.data.deleteDocument('companies', companyId);
                } catch (error) {
                     console.error(`Erro ao excluir o documento da empresa ou sua configuração:`, error);
                     errors.push('company/config');
                }


                App.ui.setLoading(false);
                if (errors.length > 0) {
                    App.ui.showAlert(`Exclusão concluída com erros nas coleções: ${errors.join(', ')}.`, "error", 10000);
                } else {
                    App.ui.showAlert(`Empresa e todos os seus ${totalDeleted} dados associados foram excluídos permanentemente.`, "success", 10000);
                }
            },

            async migrateOldData() {
                if (App.state.currentUser.role !== 'super-admin') {
                    App.ui.showAlert("Apenas super administradores podem executar esta ação.", "error");
                    return;
                }

                const activeCompanies = App.state.companies.filter(c => c.active !== false);

                if (!activeCompanies || activeCompanies.length === 0) {
                    App.ui.showAlert("Nenhuma empresa ativa encontrada. Por favor, crie e ative uma empresa primeiro.", "error");
                    return;
                }

                const companyOptions = [
                    { value: "", text: "Selecione uma empresa..." }, // Placeholder
                    ...activeCompanies.map(c => ({
                        value: c.id,
                        text: `${c.name} (ID: ${c.id.substring(0, 5)}...)`
                    }))
                ];

                const confirmationMessage = `Selecione a empresa de destino para migrar todos os dados antigos (sem empresa associada). Esta ação não pode ser desfeita.`;

                App.ui.showConfirmationModal(
                    confirmationMessage,
                    async (results) => {
                        const targetCompanyId = results.companySelect;
                        if (!targetCompanyId) {
                            App.ui.showAlert("Nenhuma empresa selecionada. Ação cancelada.", "warning");
                            return;
                        }
                        const targetCompany = activeCompanies.find(c => c.id === targetCompanyId);

                        App.ui.setLoading(true, "A iniciar migração de dados...");

                        const collectionsToMigrate = ['users', 'fazendas', 'personnel', 'registros', 'perdas', 'planos', 'harvestPlans', 'armadilhas', 'cigarrinha', 'cigarrinhaAmostragem'];
                        let totalMigratedCount = 0;
                        const errors = [];

                        for (const collectionName of collectionsToMigrate) {
                            try {
                                App.ui.setLoading(true, `A verificar a coleção: ${collectionName}...`);
                                // Corrigido: a consulta por 'null' pode não funcionar como esperado.
                                // É mais seguro consultar por ausência do campo.
                                const q = query(collection(db, collectionName));
                                const snapshot = await getDocs(q);

                                const docsToMigrate = snapshot.docs.filter(doc => !doc.data().companyId);

                                if (docsToMigrate.length === 0) {
                                    console.log(`Nenhum documento para migrar em '${collectionName}'.`);
                                    continue;
                                }

                                const batchSize = 400;
                                const chunks = [];
                                for (let i = 0; i < docsToMigrate.length; i += batchSize) {
                                    chunks.push(docsToMigrate.slice(i, i + batchSize));
                                }

                                for (const chunk of chunks) {
                                    const batch = writeBatch(db);
                                    chunk.forEach(doc => {
                                        batch.update(doc.ref, { companyId: targetCompanyId });
                                    });
                                    await batch.commit();
                                    totalMigratedCount += chunk.length;
                                    App.ui.setLoading(true, `${totalMigratedCount} documentos migrados...`);
                                }
                            } catch (error) {
                                console.error(`Erro ao migrar a coleção ${collectionName}:`, error);
                                errors.push(collectionName);
                            }
                        }

                        App.ui.setLoading(false);
                        if (errors.length > 0) {
                            App.ui.showAlert(`Migração concluída com erros nas coleções: ${errors.join(', ')}. Total migrado: ${totalMigratedCount}.`, "error", 10000);
                        } else if (totalMigratedCount > 0) {
                            App.ui.showAlert(`Migração concluída! ${totalMigratedCount} documentos foram associados à empresa ${targetCompany.name}.`, "success", 10000);
                        } else {
                            App.ui.showAlert("Nenhum documento precisava de ser migrado.", "info");
                        }
                    },
                    [{
                        type: 'select',
                        id: 'companySelect',
                        label: 'Selecione a Empresa de Destino',
                        options: companyOptions,
                        required: true
                    }]
                );
            },

            async editHarvestPlan(planId = null) {
                App.ui.showHarvestPlanEditor();
                const { frontName, startDate, dailyRate } = App.elements.harvest;
                
                if (planId) {
                    const planToEdit = App.state.harvestPlans.find(p => p.id == planId);
                    App.state.activeHarvestPlan = JSON.parse(JSON.stringify(planToEdit));
                } else {
                    let targetCompanyId = App.state.currentUser.companyId;
                    if (App.state.currentUser.role === 'super-admin') {
                        targetCompanyId = App.elements.harvest.adminTargetCompanyHarvest.value;
                        if (!targetCompanyId) {
                            App.ui.showAlert("Como Super Admin, você deve selecionar uma empresa alvo para criar um novo plano.", "error");
                            App.ui.showHarvestPlanList(); // Volta para a lista
                            return;
                        }
                    }
                    App.state.activeHarvestPlan = {
                        frontName: '',
                        startDate: new Date().toISOString().split('T')[0],
                        dailyRate: 750,
                        sequence: [],
                        closedTalhaoIds: [],
                        companyId: targetCompanyId
                    };
                }

                try {
                    const userId = App.state.currentUser.uid;
                    App.state.activeHarvestPlan.draftTimestamp = new Date().toISOString();
                    await App.data.setDocument('userDrafts', userId, App.state.activeHarvestPlan);
                } catch (error) {
                    console.error("Não foi possível guardar o rascunho no Firestore:", error);
                }
                
                frontName.value = App.state.activeHarvestPlan.frontName;
                startDate.value = App.state.activeHarvestPlan.startDate;
                dailyRate.value = App.state.activeHarvestPlan.dailyRate;

                App.ui.renderHarvestSequence();
                App.ui.populateFazendaSelects();
                this.cancelEditSequence();
            },
            updateActiveHarvestPlanDetails() {
                if (!App.state.activeHarvestPlan) return;
                const { frontName, startDate, dailyRate } = App.elements.harvest;
                App.state.activeHarvestPlan.frontName = frontName.value;
                App.state.activeHarvestPlan.startDate = startDate.value;
                App.state.activeHarvestPlan.dailyRate = parseFloat(dailyRate.value);
                App.ui.renderHarvestSequence();
            },
            async saveHarvestPlan() {
                if (!App.state.activeHarvestPlan) return;
                
                App.ui.showConfirmationModal("Tem a certeza que deseja guardar este plano de colheita?", async () => {
                    const planToSave = App.state.activeHarvestPlan;
                    planToSave.frontName = planToSave.frontName.trim();
                    
                    if (!planToSave.frontName || !planToSave.startDate || !planToSave.dailyRate) {
                        App.ui.showAlert('Preencha todos os campos de configuração da frente.', "error");
                        return;
                    }
                    
                    try {
                        if (planToSave.id) {
                            await App.data.setDocument('harvestPlans', planToSave.id, planToSave);
                        } else {
                            await App.data.addDocument('harvestPlans', planToSave);
                        }
                        App.ui.showAlert(`Plano de colheita "${planToSave.frontName}" guardado com sucesso!`);
                        App.ui.showHarvestPlanList();
                    } catch(e) {
                        App.ui.showAlert('Erro ao guardar o plano de colheita.', "error");
                    }
                });
            },
            deleteHarvestPlan(planId) {
                App.ui.showConfirmationModal("Tem a certeza que deseja excluir este plano de colheita?", async () => {
                    await App.data.deleteDocument('harvestPlans', planId);
                    App.ui.showAlert('Plano de colheita excluído.', 'info');
                });
            },
            addOrUpdateHarvestSequence() {
                if (!App.state.activeHarvestPlan) { App.ui.showAlert("Primeiro crie ou edite um plano.", "warning"); return; }
                const { fazenda: fazendaSelect, atr: atrInput, editingGroupId, maturador, maturadorDate } = App.elements.harvest;
                const farmId = fazendaSelect.value;
                const atr = parseFloat(atrInput.value);
                const maturadorValue = maturador.value.trim();
                const maturadorDateValue = maturadorDate.value;
                const isEditing = editingGroupId.value !== '';

                if (!farmId) { App.ui.showAlert("Selecione uma fazenda.", "warning"); return; }
                if (isNaN(atr) || atr <= 0) { App.ui.showAlert("Insira um valor de ATR válido.", "warning"); return; }

                const selectedCheckboxes = document.querySelectorAll('#harvestTalhaoSelectionList input[type="checkbox"]:checked');
                if (selectedCheckboxes.length === 0) { App.ui.showAlert("Selecione pelo menos um talhão.", "warning"); return; }

                const farm = App.state.fazendas.find(f => f.id === farmId);
                if (!farm) return;

                const selectedPlots = [];
                let totalArea = 0;
                let totalProducao = 0;

                selectedCheckboxes.forEach(cb => {
                    const talhaoId = parseInt(cb.dataset.talhaoId);
                    const talhao = farm.talhoes.find(t => t.id === talhaoId);
                    if (talhao) {
                        selectedPlots.push({ talhaoId: talhao.id, talhaoName: talhao.name });
                        totalArea += talhao.area;
                        totalProducao += talhao.producao;
                    }
                });

                if (isEditing) {
                    const group = App.state.activeHarvestPlan.sequence.find(g => g.id == editingGroupId.value);
                    if (group) {
                        group.plots = selectedPlots;
                        group.totalArea = totalArea;
                        group.totalProducao = totalProducao;
                        group.atr = atr;
                        group.maturador = maturadorValue;
                        group.maturadorDate = maturadorDateValue;
                    }
                } else {
                    App.state.activeHarvestPlan.sequence.push({
                        id: Date.now(), fazendaCodigo: farm.code, fazendaName: farm.name,
                        plots: selectedPlots, totalArea, totalProducao, atr,
                        maturador: maturadorValue,
                        maturadorDate: maturadorDateValue
                    });
                }
                
                App.ui.renderHarvestSequence();
                this.cancelEditSequence();
            },
            editHarvestSequenceGroup(groupId) {
                if (!App.state.activeHarvestPlan) return;
                const { fazenda, atr, editingGroupId, btnAddOrUpdate, btnCancelEdit, addOrEditTitle, maturador, maturadorDate } = App.elements.harvest;
                const group = App.state.activeHarvestPlan.sequence.find(g => g.id == groupId);
                if (!group) return;

                editingGroupId.value = group.id;

                // Garante que o select da fazenda é populado com a fazenda a ser editada incluída
                App.ui.populateFazendaSelects();

                const farm = App.state.fazendas.find(f => f.code === group.fazendaCodigo);

                // Define o valor do select APÓS ter sido populado
                fazenda.value = farm ? farm.id : "";
                fazenda.disabled = true;
                atr.value = group.atr;
                maturador.value = group.maturador || '';
                maturadorDate.value = group.maturadorDate || '';
                
                const plotIds = group.plots.map(p => p.talhaoId);
                App.ui.renderHarvestTalhaoSelection(farm.id, plotIds);

                addOrEditTitle.innerHTML = `<i class="fas fa-edit"></i> Editar Sequência da Fazenda`;
                btnAddOrUpdate.innerHTML = `<i class="fas fa-save"></i> Atualizar Sequência`;
                btnCancelEdit.style.display = 'inline-flex';
                
                fazenda.scrollIntoView({ behavior: 'smooth', block: 'center' });
            },
            cancelEditSequence() {
                const { fazenda, atr, editingGroupId, btnAddOrUpdate, btnCancelEdit, addOrEditTitle, talhaoSelectionList, maturador, maturadorDate } = App.elements.harvest;
                editingGroupId.value = '';
                fazenda.value = '';
                fazenda.disabled = false;
                atr.value = '';
                maturador.value = '';
                maturadorDate.value = '';
                talhaoSelectionList.innerHTML = '';
                addOrEditTitle.innerHTML = `<i class="fas fa-plus-circle"></i> Adicionar Fazenda à Sequência`;
                btnAddOrUpdate.innerHTML = `<i class="fas fa-plus"></i> Adicionar à Sequência`;
                btnCancelEdit.style.display = 'none';
            },
            removeHarvestSequence(groupId) {
                if (!App.state.activeHarvestPlan) return;
                
                App.ui.showConfirmationModal("Tem a certeza que deseja remover este grupo da sequência?", () => {
                    App.state.activeHarvestPlan.sequence = App.state.activeHarvestPlan.sequence.filter(g => g.id != groupId);
                    
                    if (App.state.activeHarvestPlan.id) {
                        const planInList = App.state.harvestPlans.find(p => p.id === App.state.activeHarvestPlan.id);
                        if (planInList) {
                            planInList.sequence = App.state.activeHarvestPlan.sequence;
                        }
                    }

                    App.ui.renderHarvestSequence();
                    App.ui.populateFazendaSelects();
                    App.actions.cancelEditSequence();
                    App.ui.showAlert('Grupo removido da sequência.', 'info');
                });
            },
            reorderHarvestSequence(draggedId, targetId) {
                if (!App.state.activeHarvestPlan) return;
                const sequence = App.state.activeHarvestPlan.sequence;
                const fromIndex = sequence.findIndex(item => item.id == draggedId);
                const toIndex = sequence.findIndex(item => item.id == targetId);
                if (fromIndex === -1 || toIndex === -1) return;
                const item = sequence.splice(fromIndex, 1)[0];
                sequence.splice(toIndex, 0, item);
                App.ui.renderHarvestSequence();
            },
            calculateAverageAge(group, groupStartDate) {
                let totalAgeInDays = 0;
                let plotsWithDate = 0;
                group.plots.forEach(plot => {
                    const farm = App.state.fazendas.find(f => f.code === group.fazendaCodigo);
                    const talhao = farm?.talhoes.find(t => t.id === plot.talhaoId);
                        if (talhao && talhao.dataUltimaColheita && groupStartDate) {
                        const dataUltima = new Date(talhao.dataUltimaColheita + 'T03:00:00Z');
                        if (!isNaN(groupStartDate) && !isNaN(dataUltima)) {
                            totalAgeInDays += Math.abs(groupStartDate - dataUltima);
                            plotsWithDate++;
                        }
                    }
                });
        
                if (plotsWithDate > 0) {
                    const avgDiffTime = totalAgeInDays / plotsWithDate;
                    const avgDiffDays = Math.ceil(avgDiffTime / (1000 * 60 * 60 * 24));
                    return (avgDiffDays / 30).toFixed(1);
                }
                return 'N/A';
            },
            calculateMaturadorDays(group) {
                if (!group.maturadorDate) {
                    return 'N/A';
                }
                try {
                    const today = new Date();
                    const applicationDate = new Date(group.maturadorDate + 'T03:00:00Z');
                    const diffTime = today - applicationDate;
                    if (diffTime < 0) return 0;
                    const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));
                    return diffDays;
                } catch (e) {
                    return 'N/A';
                }
            },
            async _saveEntry(config) {
                const {
                    formType,
                    formFieldIds,
                    entryBuilder,
                    collectionName,
                    confirmationMessage,
                    successMessage,
                    requiresOperatorValidation = false
                } = config;

                // 1. Validation
                if (!App.ui.validateFields(formFieldIds)) {
                    App.ui.showAlert("Preencha todos os campos obrigatórios!", "error");
                    return;
                }

                const formElements = App.elements[formType];
                const farm = App.state.fazendas.find(f => f.id === formElements.codigo.value);
                if (!farm) {
                    App.ui.showAlert("Fazenda não encontrada.", "error");
                    return;
                }

                const talhaoName = formElements.talhao.value.trim().toUpperCase();
                const talhao = farm.talhoes.find(t => t.name.toUpperCase() === talhaoName);
                if (!talhao) {
                    App.ui.showAlert(`Talhão "${formElements.talhao.value}" não encontrado na fazenda "${farm.name}". Verifique o cadastro.`, "error");
                    return;
                }

                let operator = null;
                if (requiresOperatorValidation) {
                    operator = App.state.personnel.find(p => p.matricula === formElements.matricula.value.trim());
                    if (!operator) {
                        App.ui.showAlert("Matrícula do operador não encontrada. Verifique o cadastro.", "error");
                        return;
                    }
                }

                // 2. Build entry
                const newEntry = entryBuilder(formElements, farm, talhao, operator);

                // 3. Confirmation and Save
                App.ui.showConfirmationModal(confirmationMessage, () => {
                    App.ui.clearForm(formElements.form);
                    this.clearFormDraft(formType); // Limpa o rascunho após a confirmação
                    App.ui.setDefaultDatesForEntryForms();
                    App.ui.setLoading(true, "A guardar...");

                    (async () => {
                        try {
                            if (navigator.onLine) {
                                await App.data.addDocument(collectionName, newEntry);
                                App.ui.showAlert(successMessage);
                                if (formType === 'broca' || formType === 'perda') {
                                    this.verificarEAtualizarPlano(formType, newEntry.codigo, newEntry.talhao);
                                }
                            } else {
                                const entryId = `offline_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
                                await OfflineDB.add('offline-writes', { id: entryId, collection: collectionName, data: newEntry });
                                App.ui.showAlert('Guardado offline. Será enviado quando houver conexão.', 'info');
                            }
                        } catch (e) {
                            App.ui.showAlert('Erro ao guardar. A guardar offline.', "error");
                            console.error(`Erro ao salvar ${formType}, salvando offline:`, e);
                            const entryId = `offline_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
                            await OfflineDB.add('offline-writes', { id: entryId, collection: collectionName, data: newEntry });
                        } finally {
                            App.ui.setLoading(false);
                        }
                    })();
                });
            },

            saveBrocamento() {
                this._saveEntry({
                    formType: 'broca',
                    formFieldIds: ['codigo', 'data', 'talhao', 'entrenos', 'brocaBase', 'brocaMeio', 'brocaTopo'],
                    collectionName: 'registros',
                    confirmationMessage: 'Tem a certeza que deseja guardar esta inspeção de broca?',
                    successMessage: 'Inspeção guardada com sucesso!',
                    entryBuilder: (els, farm, talhao) => ({
                        codigo: farm.code, fazenda: farm.name, data: els.data.value,
                        talhao: els.talhao.value.trim(),
                        corte: talhao ? talhao.corte : null,
                        entrenos: parseInt(els.entrenos.value),
                        base: parseInt(els.base.value),
                        meio: parseInt(els.meio.value),
                        topo: parseInt(els.topo.value),
                        brocado: parseInt(els.brocado.value),
                        brocamento: (((parseInt(els.brocado.value) || 0) / (parseInt(els.entrenos.value) || 1)) * 100).toFixed(2).replace('.', ','),
                        usuario: App.state.currentUser.username,
                        companyId: App.state.currentUser.companyId
                    })
                });
            },
            
            saveCigarrinha() {
                this._saveEntry({
                    formType: 'cigarrinha',
                    formFieldIds: ['dataCigarrinha', 'codigoCigarrinha', 'talhaoCigarrinha'],
                    collectionName: 'cigarrinha',
                    confirmationMessage: 'Tem a certeza que deseja guardar este monitoramento?',
                    successMessage: 'Monitoramento guardado com sucesso!',
                    entryBuilder: (els, farm, talhao) => {
                        const f1 = parseInt(els.fase1.value) || 0;
                        const f2 = parseInt(els.fase2.value) || 0;
                        const f3 = parseInt(els.fase3.value) || 0;
                        const f4 = parseInt(els.fase4.value) || 0;
                        const f5 = parseInt(els.fase5.value) || 0;
                        const divisor = parseInt(App.state.companyConfig?.cigarrinhaCalcMethod || '5', 10);
                        return {
                            data: els.data.value,
                            codigo: farm.code,
                            fazenda: farm.name,
                            talhao: els.talhao.value.trim(),
                            variedade: talhao.variedade || '',
                            fase1: f1, fase2: f2, fase3: f3, fase4: f4, fase5: f5,
                            adulto: els.adulto.checked,
                            resultado: (f1 + f2 + f3 + f4 + f5) / divisor,
                            usuario: App.state.currentUser.username,
                            companyId: App.state.currentUser.companyId
                        };
                    }
                });
            },

            saveCigarrinhaAmostragem() {
                const els = App.elements.cigarrinhaAmostragem;
                if (!App.ui.validateFields([els.data.id, els.codigo.id, els.talhao.id])) {
                    App.ui.showAlert("Preencha todos os campos principais (Data, Fazenda, Talhão)!", "error");
                    return;
                }

                const amostrasCards = els.amostrasContainer.querySelectorAll('.amostra-card');
                if (amostrasCards.length === 0) {
                    App.ui.showAlert("Adicione pelo menos uma sub-amostra antes de guardar.", "error");
                    return;
                }

                const farm = App.state.fazendas.find(f => f.id === els.codigo.value);
                const talhao = farm?.talhoes.find(t => t.name.toUpperCase() === els.talhao.value.trim().toUpperCase());

                const amostrasData = [];
                amostrasCards.forEach(card => {
                    const amostra = {};
                    card.querySelectorAll('.amostra-input').forEach((input, index) => {
                        amostra[`fase${index + 1}`] = parseInt(input.value) || 0;
                    });
                    amostrasData.push(amostra);
                });

                const divisor = parseInt(App.state.companyConfig?.cigarrinhaCalcMethod || '5', 10);
                const somaDasMedias = amostrasData.reduce((acc, amostra) => {
                    const somaFases = Object.values(amostra).reduce((sum, val) => sum + val, 0);
                    return acc + (somaFases / divisor);
                }, 0);
                const mediaFinal = somaDasMedias / amostrasData.length;

                const newEntry = {
                    data: els.data.value,
                    codigo: farm.code,
                    fazenda: farm.name,
                    talhao: talhao.name,
                    variedade: talhao.variedade || '',
                    adulto: els.adulto.checked,
                    resultado: mediaFinal,
                    amostras: amostrasData,
                    divisor: divisor,
                    usuario: App.state.currentUser.username,
                    companyId: App.state.currentUser.companyId
                };

                App.ui.showConfirmationModal("Tem a certeza que deseja guardar este lançamento?", () => {
                    App.ui.clearForm(els.form);
                    els.amostrasContainer.innerHTML = '';
                    App.ui.setDefaultDatesForEntryForms();
                    App.ui.setLoading(true, "A guardar...");

                    (async () => {
                        try {
                            if (navigator.onLine) {
                                await App.data.addDocument('cigarrinhaAmostragem', newEntry);
                                App.ui.showAlert("Lançamento de amostragem guardado com sucesso!");
                            } else {
                                const entryId = `offline_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
                                await OfflineDB.add('offline-writes', { id: entryId, collection: 'cigarrinhaAmostragem', data: newEntry });
                                App.ui.showAlert('Guardado offline. Será enviado quando houver conexão.', 'info');
                            }
                        } catch (e) {
                            App.ui.showAlert('Erro ao guardar. A guardar offline.', "error");
                            console.error(`Erro ao salvar cigarrinhaAmostragem, salvando offline:`, e);
                            const entryId = `offline_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
                            await OfflineDB.add('offline-writes', { id: entryId, collection: 'cigarrinhaAmostragem', data: newEntry });
                        } finally {
                            App.ui.setLoading(false);
                        }
                    })();
                });
            },

            savePerda() {
                this._saveEntry({
                    formType: 'perda',
                    formFieldIds: ['dataPerda', 'codigoPerda', 'frenteServico', 'talhaoPerda', 'frotaEquipamento', 'matriculaOperador'],
                    collectionName: 'perdas',
                    confirmationMessage: 'Tem a certeza que deseja guardar este lançamento de perda?',
                    successMessage: 'Lançamento de perda guardado com sucesso!',
                    requiresOperatorValidation: true,
                    entryBuilder: (els, farm, talhao, operator) => {
                        const fields = { canaInteira: parseFloat(els.canaInteira.value) || 0, tolete: parseFloat(els.tolete.value) || 0, toco: parseFloat(els.toco.value) || 0, ponta: parseFloat(els.ponta.value) || 0, estilhaco: parseFloat(els.estilhaco.value) || 0, pedaco: parseFloat(els.pedaco.value) || 0 };
                        const total = Object.values(fields).reduce((s, v) => s + v, 0);
                        return {
                            ...fields,
                            data: els.data.value,
                            codigo: farm.code,
                            fazenda: farm.name,
                            frenteServico: els.frente.value.trim(),
                            turno: els.turno.value,
                            talhao: els.talhao.value.trim(),
                            frota: els.frota.value.trim(),
                            matricula: operator.matricula,
                            operador: operator.name,
                            total,
                            media: (total / 6).toFixed(2).replace('.', ','),
                            usuario: App.state.currentUser.username,
                            companyId: App.state.currentUser.companyId
                        };
                    }
                });
            },
            
            editEntry(type, id) {
                if (type === 'apontamentoPlantio') {
                    const entry = App.state.apontamentosPlantio.find(e => e.id === id);
                    if (entry) {
                        App.ui.showTab('apontamentoPlantio');
                        const els = App.elements.apontamentoPlantio;
                        els.entryId.value = id;
                        els.frente.value = entry.frenteDePlantioId;
                        els.provider.value = entry.provider;
                        els.leaderId.value = entry.leaderId;
                        els.farmName.value = App.state.fazendas.find(f => f.code === entry.farmCode).id;
                        els.date.value = entry.date;
                        els.chuva.value = entry.chuva;
                        els.obs.value = entry.obs;
                        els.recordsContainer.innerHTML = '';
                        entry.records.forEach(record => {
                            App.ui.addPlantioRecordCard();
                            const card = els.recordsContainer.lastChild;
                            const talhaoSelect = card.querySelector('.plantio-talhao-select');
                            const variedadeInput = card.querySelector('input[id^="plantioVariedade-"]');
                            const areaInput = card.querySelector('input[id^="plantioArea-"]');
                            talhaoSelect.value = record.talhaoId;
                            variedadeInput.value = record.variedade;
                            areaInput.value = record.area;
                            App.ui.updateTalhaoInfo(card);
                        });
                        App.ui.calculateTotalPlantedArea();
                    }
                }
            },

            deleteEntry(type, id) {
                App.ui.showConfirmationModal('Tem a certeza que deseja excluir este registo?', async () => {
                    if (type === 'brocamento') { await App.data.deleteDocument('registros', id); }
                    else if (type === 'perda') { await App.data.deleteDocument('perdas', id); }
                    else if (type === 'apontamentoPlantio') { await App.data.deleteDocument('apontamentosPlantio', id); }
                    App.ui.showAlert('Registo excluído com sucesso!');
                });
            },
            async importFarmsFromCSV(file) {
                 if (!file) return;
                 const reader = new FileReader();
                 const progressEls = App.elements.cadastros.importProgress;

                 reader.onload = async (event) => {
                     const CHUNK_SIZE = 400; 
                     const PAUSE_DURATION = 50;
                     
                     try {
                         const csv = event.target.result;
                         const lines = csv.split(/\r\n|\n/).filter(line => line.trim() !== '');
                         const totalLines = lines.length - 1;

                         if (totalLines <= 0) {
                             App.ui.showAlert('O ficheiro CSV está vazio ou contém apenas o cabeçalho.', "error"); return;
                         }
                         
                         progressEls.container.style.display = 'block';
                         progressEls.text.textContent = `A iniciar importação de ${totalLines} linhas...`;
                         progressEls.bar.value = 0;
                         progressEls.bar.max = totalLines;
                         await new Promise(resolve => setTimeout(resolve, 100));

                         const fileHeaders = lines[0].split(';').map(h => h.trim().toUpperCase().normalize("NFD").replace(/[\u0300-\u036f]/g, ""));
                         const headerIndexes = {
                             farm_code: fileHeaders.indexOf('COD'), farm_name: fileHeaders.indexOf('FAZENDA'),
                             farm_type: fileHeaders.indexOf('TIPO'),
                             talhao_name: fileHeaders.indexOf('TALHAO'), talhao_area: fileHeaders.indexOf('AREA'),
                             talhao_tch: fileHeaders.indexOf('TCH'),
                             talhao_variedade: fileHeaders.indexOf('VARIEDADE'),
                             talhao_corte: fileHeaders.indexOf('CORTE'),
                             talhao_distancia: fileHeaders.indexOf('DISTANCIA'),
                             talhao_ultima_colheita: fileHeaders.indexOf('DATAULTIMACOLHEITA'),
                         };

                         if (headerIndexes.farm_code === -1 || headerIndexes.farm_name === -1 || headerIndexes.talhao_name === -1) {
                             throw new Error('Cabeçalhos essenciais (Cód;FAZENDA;TALHÃO) não encontrados no ficheiro CSV.');
                         }
                         
                         const fazendasToUpdate = {};
                         for (let i = 1; i < lines.length; i++) {
                             const data = lines[i].split(';');
                             if (data.length < 2) continue;
                             const farmCode = data[headerIndexes.farm_code]?.trim();
                             if (!farmCode) continue;

                             if (!fazendasToUpdate[farmCode]) {
                                 let existingFarm = App.state.fazendas.find(f => f.code === farmCode);
                                 let targetCompanyId = App.state.currentUser.companyId;
                                 if(App.state.currentUser.role === 'super-admin') {
                                    targetCompanyId = App.elements.cadastros.adminTargetCompanyFarms.value;
                                    if(!targetCompanyId) {
                                         throw new Error("Como Super Admin, você deve selecionar uma empresa alvo para importar os dados.");
                                    }
                                 }

                                 fazendasToUpdate[farmCode] = existingFarm ? JSON.parse(JSON.stringify(existingFarm)) : {
                                     code: farmCode,
                                     name: data[headerIndexes.farm_name]?.trim().toUpperCase() || `FAZENDA ${farmCode}`,
                                     types: data[headerIndexes.farm_type]?.trim().split(',').map(t => t.trim()) || [],
                                     talhoes: [],
                                     companyId: targetCompanyId
                                 };
                             }

                             const talhaoName = data[headerIndexes.talhao_name]?.trim().toUpperCase();
                             if(!talhaoName) continue;

                             let talhao = fazendasToUpdate[farmCode].talhoes.find(t => t.name.toUpperCase() === talhaoName);
                             const area = parseFloat(data[headerIndexes.talhao_area]?.trim().replace(',', '.')) || 0;
                             const tch = parseFloat(data[headerIndexes.talhao_tch]?.trim().replace(',', '.')) || 0;
                             const producao = area * tch;

                             if (talhao) { 
                                 talhao.area = area;
                                 talhao.tch = tch;
                                 talhao.producao = producao;
                                 talhao.variedade = data[headerIndexes.talhao_variedade]?.trim() || talhao.variedade;
                                 talhao.corte = parseInt(data[headerIndexes.talhao_corte]?.trim()) || talhao.corte;
                                 talhao.distancia = parseFloat(data[headerIndexes.talhao_distancia]?.trim().replace(',', '.')) || talhao.distancia;
                                 talhao.dataUltimaColheita = this.formatDateForInput(data[headerIndexes.talhao_ultima_colheita]?.trim()) || talhao.dataUltimaColheita;
                             } else { 
                                 fazendasToUpdate[farmCode].talhoes.push({
                                     id: Date.now() + i, name: talhaoName,
                                     area: area,
                                     tch: tch,
                                     producao: producao,
                                     variedade: data[headerIndexes.talhao_variedade]?.trim() || '',
                                     corte: parseInt(data[headerIndexes.talhao_corte]?.trim()) || 1,
                                     distancia: parseFloat(data[headerIndexes.talhao_distancia]?.trim().replace(',', '.')) || 0,
                                     dataUltimaColheita: this.formatDateForInput(data[headerIndexes.talhao_ultima_colheita]?.trim()) || '',
                                 });
                             }
                         }
                         
                         const farmCodes = Object.keys(fazendasToUpdate);
                         for (let i = 0; i < farmCodes.length; i += CHUNK_SIZE) {
                             const chunk = farmCodes.slice(i, i + CHUNK_SIZE);
                             const batch = writeBatch(db);
                             
                             chunk.forEach(code => {
                                 const farmData = fazendasToUpdate[code];
                                 const docRef = farmData.id ? doc(db, 'fazendas', farmData.id) : doc(collection(db, 'fazendas'));
                                 batch.set(docRef, farmData, { merge: true });
                             });

                             await batch.commit();
                             const progress = Math.min(i + CHUNK_SIZE, farmCodes.length);
                             progressEls.bar.value = progress;
                             progressEls.text.textContent = `A processar... ${progress} de ${farmCodes.length} fazendas atualizadas.`;
                             await new Promise(resolve => setTimeout(resolve, PAUSE_DURATION));
                         }

                         App.ui.showAlert(`Importação concluída! ${farmCodes.length} fazendas foram processadas.`, 'success');

                     } catch (e) {
                         App.ui.showAlert(`Erro ao processar o ficheiro CSV: ${e.message}`, "error", 5000);
                         console.error(e);
                     } finally {
                         setTimeout(() => {
                            progressEls.container.style.display = 'none';
                         }, 4000);
                         App.elements.cadastros.csvFileInput.value = '';
                     }
                 };
                 reader.readAsText(file, 'ISO-8859-1');
            },
            downloadCsvTemplate() {
                const headers = "Cód;FAZENDA;TIPO;TALHAO;Área;TCH;Variedade;Corte;Distancia;DataUltimaColheita";
                const exampleRow = "4012;FAZ LAGOA CERCADA;Própria,Parceira;T-01;50;80;RB867515;2;10;15/07/2024";
                const csvContent = "\uFEFF" + headers + "\n" + exampleRow;
                const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
                const url = URL.createObjectURL(blob);
                const link = document.createElement("a");
                link.setAttribute("href", url);
                link.setAttribute("download", "modelo_cadastro_fazendas.csv");
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
            },

            downloadHistoricalReportTemplate() {
                const headers = "CodigoFazenda;Toneladas;ATR";
                const exampleRow = "4012;5000;135.50";
                const csvContent = "\uFEFF" + headers + "\n" + exampleRow;
                const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
                const url = URL.createObjectURL(blob);
                const link = document.createElement("a");
                link.setAttribute("href", url);
                link.setAttribute("download", "modelo_historico_atr.csv");
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
            },
            async importPersonnelFromCSV(file) {
                 if (!file) return;
                 const reader = new FileReader();
                 reader.onload = async (event) => {
                     const CHUNK_SIZE = 400;
                     const PAUSE_DURATION = 50;
                     try {
                         const csv = event.target.result;
                         const lines = csv.split(/\r\n|\n/).filter(line => line.trim() !== '');
                         const totalLines = lines.length - 1;
                         if (totalLines <= 0) { App.ui.showAlert('O ficheiro CSV está vazio ou contém apenas o cabeçalho.', "error"); return; }
                         
                         App.ui.setLoading(true, `A iniciar importação de ${totalLines} pessoas...`);
                         await new Promise(resolve => setTimeout(resolve, 100));

                         const fileHeaders = lines[0].split(';').map(h => h.trim().toUpperCase());
                         const headerIndexes = { matricula: fileHeaders.indexOf('MATRICULA'), name: fileHeaders.indexOf('NOME') };

                         if (headerIndexes.matricula === -1 || headerIndexes.name === -1) {
                             App.ui.showAlert('Cabeçalhos "Matricula" e "Nome" não encontrados.', "error");
                             App.ui.setLoading(false);
                             return;
                         }

                         const localPersonnel = JSON.parse(JSON.stringify(App.state.personnel));
                         
                         for (let i = 1; i < lines.length; i += CHUNK_SIZE) {
                             const chunk = lines.slice(i, i + CHUNK_SIZE);
                             const batch = writeBatch(db);
                             let updatedCountInChunk = 0;
                             let newCountInChunk = 0;

                             chunk.forEach(line => {
                                 const data = line.split(';');
                                 if (data.length < 2) return;
                                 const matricula = data[headerIndexes.matricula]?.trim();
                                 const name = data[headerIndexes.name]?.trim();
                                 if (!matricula || !name) return;

                                 let person = localPersonnel.find(p => p.matricula === matricula);
                                 if (person) {
                                     const personRef = doc(db, 'personnel', person.id);
                                     batch.update(personRef, { name: name });
                                     updatedCountInChunk++;
                                 } else {
                                     const newPersonRef = doc(collection(db, 'personnel'));
                                 batch.set(newPersonRef, { matricula, name, companyId: App.state.currentUser.companyId }); // FIX: Adicionar companyId
                                     newCountInChunk++;
                                 }
                             });

                             await batch.commit();
                             const progress = Math.min(i + CHUNK_SIZE - 1, totalLines);
                             App.ui.setLoading(true, `A processar... ${progress} de ${totalLines} pessoas.`);
                             await new Promise(resolve => setTimeout(resolve, PAUSE_DURATION));
                         }
                         
                         App.ui.showAlert(`Importação concluída!`, 'success');
                     } catch (e) {
                         App.ui.showAlert('Erro ao processar o ficheiro CSV.', "error");
                         console.error(e);
                     } finally {
                         App.ui.setLoading(false);
                         App.elements.personnel.csvFileInput.value = '';
                     }
                 };
                 reader.readAsText(file, 'ISO-8859-1');
            },
            downloadPersonnelCsvTemplate() {
                const headers = "Matricula;Nome";
                const exampleRow = "12345;José Almeida";
                const csvContent = "\uFEFF" + headers + "\n" + exampleRow;
                const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
                const url = URL.createObjectURL(blob);
                const link = document.createElement("a");
                link.setAttribute("href", url);
                link.setAttribute("download", "modelo_cadastro_pessoas.csv");
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
            },
            downloadHarvestReportTemplate(type) {
                let headers, exampleRow, filename;
                if (type === 'progress') {
                    headers = "CodigoFazenda;Talhao;AreaColhida;ProducaoColhida";
                    exampleRow = "4012;T-01;10.5;850.7";
                    filename = "modelo_colheita_andamento.csv";
                } else { // closed
                    headers = "CodigoFazenda;Talhao";
                    exampleRow = "4012;T-02";
                    filename = "modelo_colheita_encerrados.csv";
                }
                const csvContent = "\uFEFF" + headers + "\n" + exampleRow;
                const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
                const url = URL.createObjectURL(blob);
                const link = document.createElement("a");
                link.setAttribute("href", url);
                link.setAttribute("download", filename);
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
            },
            async uploadHistoricalReport(file) {
                if (!file) return;
                const reader = new FileReader();
                reader.onload = async (event) => {
                    const reportData = event.target.result;
                    App.ui.setLoading(true, "A enviar relatório para análise da IA...");
                    try {
                        const response = await fetch(`${App.config.backendUrl}/api/upload/historical-report`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                reportData,
                                companyId: App.state.currentUser.companyId
                            }),
                        });
                        const result = await response.json();
                        if (!response.ok) {
                            throw new Error(result.message || 'Erro no servidor');
                        }
                        App.ui.showAlert(result.message, 'success');
                    } catch (error) {
                        App.ui.showAlert(`Erro ao enviar relatório: ${error.message}`, 'error');
                    } finally {
                        App.ui.setLoading(false);
                        App.elements.companyConfig.historicalReportInput.value = '';
                    }
                };
                reader.readAsDataURL(file);
            },

            async deleteHistoricalData() {
                const confirmationText = "EXCLUIR HISTORICO";
                App.ui.showConfirmationModal(
                    `Esta ação é irreversível e irá apagar TODOS os dados históricos de colheita que a IA usa para previsões. Para confirmar, digite "${confirmationText}" no campo abaixo.`,
                    async (userInput) => {
                        if (userInput.confirmationModalInput !== confirmationText) {
                            App.ui.showAlert("A confirmação não corresponde. Ação cancelada.", "warning");
                            return;
                        }

                        App.ui.setLoading(true, "A apagar histórico...");
                        try {
                            const response = await fetch(`${App.config.backendUrl}/api/delete/historical-data`, {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({ companyId: App.state.currentUser.companyId })
                            });
                            const result = await response.json();
                            if (!response.ok) {
                                throw new Error(result.message || 'Erro no servidor');
                            }
                            App.ui.showAlert(result.message, 'success');
                        } catch (error) {
                            App.ui.showAlert(`Erro ao apagar o histórico: ${error.message}`, 'error');
                        } finally {
                            App.ui.setLoading(false);
                        }
                    },
                    [{ id: 'confirmationModalInput', placeholder: `Digite "${confirmationText}"`, required: true }]
                );
            },

            async importHarvestReport(file, type) {
                if (!file) return;
        
                const reader = new FileReader();
                reader.onload = async (event) => {
                    App.ui.setLoading(true, `A processar relatório de talhões ${type === 'closed' ? 'encerrados' : 'em andamento'}...`);
                    try {
                        const csv = event.target.result;
                        const lines = csv.split(/\r\n|\n/).filter(line => line.trim() !== '');
                        if (lines.length <= 1) throw new Error("O ficheiro CSV está vazio ou contém apenas o cabeçalho.");
        
                        const headers = lines[0].split(';').map(h => h.trim().toLowerCase());
                        const requiredHeaders = type === 'progress' ? ['codigofazenda', 'talhao', 'areacolhida', 'producaocolhida'] : ['codigofazenda', 'talhao'];
                        if (!requiredHeaders.every(h => headers.includes(h))) {
                            throw new Error(`Cabeçalhos em falta. O ficheiro deve conter: ${requiredHeaders.join('; ')}`);
                        }
        
                        const allPlans = JSON.parse(JSON.stringify(App.state.harvestPlans));
                        const fazendas = App.state.fazendas;
                        const changesSummary = {};
                        let notFoundTalhoes = [];
        
                        const closedTalhaoIdsFromCSV = new Set();
                        if (type === 'closed') {
                            for (let i = 1; i < lines.length; i++) {
                                const data = lines[i].split(';');
                                const row = headers.reduce((obj, header, index) => { obj[header] = data[index]?.trim(); return obj; }, {});
                                const farmCode = row.codigofazenda;
                                const talhaoName = row.talhao?.toUpperCase();
                                if (farmCode && talhaoName) {
                                    const farm = fazendas.find(f => f.code === farmCode);
                                    const talhao = farm?.talhoes.find(t => t.name.toUpperCase() === talhaoName);
                                    if (talhao) {
                                        closedTalhaoIdsFromCSV.add(talhao.id);
                                    }
                                }
                            }
                        }
        
                        if (type === 'progress') {
                            for (let i = 1; i < lines.length; i++) {
                                const data = lines[i].split(';');
                                const row = headers.reduce((obj, header, index) => { obj[header] = data[index]?.trim(); return obj; }, {});
                                const farmCode = row.codigofazenda;
                                const talhaoName = row.talhao?.toUpperCase();
                                if (!farmCode || !talhaoName) continue;
        
                                const farm = fazendas.find(f => f.code === farmCode);
                                const talhao = farm?.talhoes.find(t => t.name.toUpperCase() === talhaoName);
                                if (!talhao) {
                                    if (!notFoundTalhoes.includes(`${farmCode}-${talhaoName}`)) notFoundTalhoes.push(`${farmCode}-${talhaoName}`);
                                    continue;
                                }
        
                                let talhaoFoundInAnyPlan = false;
                                for (const plan of allPlans) {
                                    for (const group of plan.sequence) {
                                        if (group.fazendaCodigo === farmCode && group.plots.some(p => p.talhaoId === talhao.id)) {
                                            talhaoFoundInAnyPlan = true;
                                            if (!changesSummary[plan.frontName]) changesSummary[plan.frontName] = { updated: [], removed: [] };
        
                                            const areaColhida = parseFloat(row.areacolhida?.replace(',', '.')) || 0;
                                            const producaoColhida = parseFloat(row.producaocolhida?.replace(',', '.')) || 0;
                                            group.areaColhida = (group.areaColhida || 0) + areaColhida;
                                            group.producaoColhida = (group.producaoColhida || 0) + producaoColhida;
                                            changesSummary[plan.frontName].updated.push(`${farmCode}-${talhaoName}`);
                                        }
                                    }
                                }
                                if (!talhaoFoundInAnyPlan && !notFoundTalhoes.includes(`${farmCode}-${talhaoName}`)) {
                                    notFoundTalhoes.push(`${farmCode}-${talhaoName}`);
                                }
                            }
                        }
        
                        if (type === 'closed') {
                            for (const plan of allPlans) {
                                if (!plan.closedTalhaoIds) plan.closedTalhaoIds = [];
                                closedTalhaoIdsFromCSV.forEach(id => {
                                    if (!plan.closedTalhaoIds.includes(id)) plan.closedTalhaoIds.push(id);
                                });
        
                                const newSequence = [];
                                plan.sequence.forEach(group => {
                                    const originalPlotCount = group.plots.length;
                                    group.plots = group.plots.filter(plot => !closedTalhaoIdsFromCSV.has(plot.talhaoId));
                                    const removedPlotsCount = originalPlotCount - group.plots.length;
        
                                    if (removedPlotsCount > 0) {
                                        if (!changesSummary[plan.frontName]) changesSummary[plan.frontName] = { updated: [], removed: [] };
                                        changesSummary[plan.frontName].removed.push(`${removedPlotsCount} talhão(ões) do grupo ${group.fazendaCodigo}`);
                                    }
        
                                    if (group.plots.length > 0) {
                                        let newTotalArea = 0;
                                        let newTotalProducao = 0;
                                        const farm = fazendas.find(f => f.code === group.fazendaCodigo);
                                        if (farm) {
                                            group.plots.forEach(plot => {
                                                const talhao = farm.talhoes.find(t => t.id === plot.talhaoId);
                                                if (talhao) {
                                                    newTotalArea += talhao.area;
                                                    newTotalProducao += talhao.producao;
                                                }
                                            });
                                        }
                                        group.totalArea = newTotalArea;
                                        group.totalProducao = newTotalProducao;
                                        newSequence.push(group);
                                    }
                                });
                                plan.sequence = newSequence;
                            }
                        }
        
                        const batch = writeBatch(db);
                        allPlans.forEach(plan => {
                            const docRef = doc(db, 'harvestPlans', plan.id);
                            batch.set(docRef, plan);
                        });
                        await batch.commit();
        
                        let summaryMessage = "Sincronização Concluída!\n\n";
                        const updatedPlans = Object.keys(changesSummary);
        
                        if (updatedPlans.length > 0) {
                            updatedPlans.forEach(planName => {
                                summaryMessage += `Plano "${planName}" atualizado:\n`;
                                const changes = changesSummary[planName];
                                if (changes.updated.length > 0) {
                                    summaryMessage += `  - ${changes.updated.length} talhões com progresso atualizado.\n`;
                                }
                                if (changes.removed.length > 0) {
                                    summaryMessage += `  - ${changes.removed.join(', ')} foram removidos da sequência.\n`;
                                }
                            });
                        } else {
                            summaryMessage += "Nenhum plano foi alterado.\n";
                        }
        
                        if (notFoundTalhoes.length > 0) {
                            summaryMessage += `\nAviso: ${notFoundTalhoes.length} talhões do relatório não foram encontrados em nenhum plano ativo: ${notFoundTalhoes.join(', ')}`;
                        }
        
                        const { confirmationModal } = App.elements;
                        confirmationModal.title.textContent = "Resumo da Sincronização";
                        confirmationModal.message.textContent = summaryMessage;
                        confirmationModal.confirmBtn.textContent = "OK";
                        confirmationModal.cancelBtn.style.display = 'none';
                        confirmationModal.overlay.classList.add('show');
                        
                        const closeHandler = () => {
                            confirmationModal.overlay.classList.remove('show');
                            confirmationModal.confirmBtn.removeEventListener('click', closeHandler);
                            confirmationModal.closeBtn.removeEventListener('click', closeHandler);
                            setTimeout(() => {
                                confirmationModal.confirmBtn.textContent = "Confirmar";
                                confirmationModal.cancelBtn.style.display = 'inline-flex';
                            }, 300);
                        };
                        confirmationModal.confirmBtn.addEventListener('click', closeHandler);
                        confirmationModal.closeBtn.addEventListener('click', closeHandler);
        
                    } catch (e) {
                        App.ui.showAlert(`Erro ao importar: ${e.message}`, "error", 6000);
                        console.error(e);
                    } finally {
                        App.ui.setLoading(false);
                        const inputToClear = type === 'progress' ? App.elements.companyConfig.progressInput : App.elements.companyConfig.closedInput;
                        if (inputToClear) inputToClear.value = '';
                    }
                };
                reader.readAsText(file, 'ISO-8859-1');
            },
            markNotificationsAsRead() {
                App.state.unreadNotificationCount = 0;
                App.ui.updateNotificationBell();
            },
            // NOVO: Ação para limpar todas as notificações
            async clearAllNotifications() {
                App.state.trapNotifications = [];
                App.state.unreadNotificationCount = 0;
                App.ui.updateNotificationBell();
                try {
                    const db = await OfflineDB.dbPromise;
                    await db.clear('notifications');
                    App.ui.showAlert("Histórico de notificações limpo.", "info");
                } catch (error) {
                    console.error("Erro ao limpar o histórico de notificações do DB:", error);
                    App.ui.showAlert("Erro ao limpar histórico.", "error");
                }
            },

            async loadNotificationHistory() {
                try {
                    const history = await OfflineDB.getAll('notifications');
                    history.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
                    App.state.trapNotifications = history;
                    App.state.unreadNotificationCount = 0; // Reset on load
                    App.ui.updateNotificationBell();
                } catch (error) {
                    console.error("Erro ao carregar histórico de notificações:", error);
                }
            },

            async saveNotification(notification) {
                // Sempre salva no IndexedDB primeiro para uma UI rápida e consistência offline
                try {
                    const db = await OfflineDB.dbPromise;
                    const tx = db.transaction('notifications', 'readwrite');
                    const store = tx.objectStore('notifications');
                    await store.add(notification);
                    const count = await store.count();
                    if (count > 20) { // Aumentado para 20
                        let cursor = await store.openCursor();
                        const toDelete = count - 20;
                        for (let i = 0; i < toDelete; i++) {
                            await cursor.delete();
                            cursor = await cursor.continue();
                        }
                    }
                    await tx.done;
                } catch (error) {
                    console.error("Erro ao salvar notificação no IndexedDB:", error);
                }

                // [CORREÇÃO] Tenta salvar no Firestore se estiver online
                if (navigator.onLine && App.state.currentUser) {
                    try {
                        const notificationData = {
                            ...notification,
                            userId: App.state.currentUser.uid, // Garante que a notificação é para o utilizador atual
                            companyId: App.state.currentUser.companyId, // Adiciona companyId para consulta
                            read: false // Estado inicial
                        };
                        // Usa addDocument para gerar um ID automático no Firestore
                        await App.data.addDocument('notifications', notificationData);
                        console.log("Notificação guardada no Firestore.");
                    } catch (error) {
                        console.error("Não foi possível guardar a notificação no Firestore (pode ser um problema de permissão ou de rede):", error);
                        // Não mostra um alerta de erro para não poluir a UI, o log no console é suficiente.
                    }
                }
            },

            async getConsolidatedData(collectionName) {
                if (!App.state[collectionName]) {
                    console.warn(`Collection ${collectionName} not found in App.state.`);
                    return [];
                }

                // 1. Get the already synced data from the state
                const syncedData = App.state[collectionName] ? [...App.state[collectionName]] : [];

                // 2. Get pending writes from IndexedDB
                const pendingWrites = await OfflineDB.getAll('offline-writes');
                const currentUserCompanyId = App.state.currentUser?.companyId;

                // 3. Filter for the specific collection and extract the data, ensuring multi-tenancy for offline data
                const pendingData = pendingWrites
                    .filter(write => write.collection === collectionName && write.data.companyId === currentUserCompanyId)
                    .map(write => ({
                        // Simulate a Firestore document by adding a temporary ID and the data
                        id: `offline_${Date.now()}_${Math.random()}`,
                        ...write.data
                    }));

                // 4. Combine and return
                return [...syncedData, ...pendingData];
            },

            async forceTokenRefresh(isManual = false) {
                if (!navigator.onLine || !auth.currentUser) {
                    if (isManual) {
                        App.ui.showSystemNotification("Sincronização", "Offline ou sem utilizador. Não é possível sincronizar.", "warning");
                    }
                    console.log("Offline ou sem utilizador, não é possível atualizar o token.");
                    return;
                }
                try {
                    const message = isManual ? "A iniciar sincronização manual..." : "Conexão reestabelecida. A iniciar sincronização automática...";
                    App.ui.showSystemNotification("Sincronização", message, "info");
                    console.log("A forçar a atualização do token de autenticação...");
                    await auth.currentUser.getIdToken(true);
                    console.log("Token de autenticação atualizado com sucesso.");

                    // FIX: Pré-carrega os dados críticos antes de reanexar os listeners para evitar a condição de corrida do menu
                    const globalConfigsDoc = await getDoc(doc(db, 'global_configs', 'main'));
                    if (globalConfigsDoc.exists()) {
                        App.state.globalConfigs = globalConfigsDoc.data();
                    }

                    if (App.state.currentUser.companyId && App.state.currentUser.role !== 'super-admin') {
                        const companyDoc = await App.data.getDocument('companies', App.state.currentUser.companyId);
                        if (companyDoc) {
                            App.state.companies = [companyDoc];
                        }
                    }

                    // Agora é seguro re-renderizar o menu
                    App.ui.renderMenu();

                    // Reinicia os 'ouvintes' de dados para usar o novo token
                    App.data.listenToAllData();

                    // Após a atualização bem-sucedida do token, iniciar a sincronização.
                    this.syncOfflineWrites();
                } catch (error) {
                    console.error("Falha ao forçar a atualização do token:", error);
                    App.ui.showSystemNotification("Erro de Autenticação", "Falha na autenticação. Não foi possível sincronizar.", "error");
                }
            },

            async syncOfflineWrites() {
                if (App.state.isSyncing) {
                    console.log("A sincronização já está em andamento.");
                    return;
                }

                App.state.isSyncing = true;
                this.syncGpsLocations(); // Sync GPS data
                console.log("Iniciando a verificação de dados offline...");

                const logEntry = {
                    userId: App.state.currentUser.uid,
                    username: App.state.currentUser.username || App.state.currentUser.email,
                    companyId: App.state.currentUser.companyId,
                    timestamp: new Date(),
                    status: '',
                    details: '',
                    items: []
                };

                try {
                    const db = await OfflineDB.dbPromise;
                    if (!db) {
                        App.state.isSyncing = false;
                        return;
                    }

                    // Etapa 1: Ler todos os dados e chaves pendentes de uma só vez
                    const writesToSync = await db.getAll('offline-writes');
                    const keysToSync = await db.getAllKeys('offline-writes');

                    if (writesToSync.length === 0) {
                        console.log("Nenhum registo pendente para sincronizar.");
                        App.state.isSyncing = false;
                        return;
                    }

                    App.ui.showSystemNotification("Sincronização", `A enviar ${writesToSync.length} registos offline...`, 'info');

                    const successfulKeys = [];
                    const unrecoverableKeys = [];
                    let successfulWrites = 0;
                    let failedWrites = 0; // Erros recuperáveis (ex: rede)
                    let discardedWrites = 0; // Erros irrecuperáveis (dados malformados)

                    // Etapa 2: Iterar sobre os dados em memória e tentar sincronizar
                    for (let i = 0; i < writesToSync.length; i++) {
                        const write = writesToSync[i];
                        const key = keysToSync[i];
                        try {
                            // Verificação de segurança para o objeto de escrita
                            if (!write || typeof write !== 'object' || !write.collection || !write.data || !write.id) {
                                throw new Error('Item de sincronização offline malformado ou inválido.');
                            }

                            let dataToSync = write.data;
                            // Handle trap installation date conversion
                            if (write.collection === 'armadilhas' && typeof write.data.dataInstalacao === 'string') {
                                dataToSync = { ...dataToSync, dataInstalacao: Timestamp.fromDate(new Date(write.data.dataInstalacao)) };
                            }
                            // Handle trap collection date conversion
                            if (write.collection === 'armadilhas' && typeof write.data.dataColeta === 'string') {
                                dataToSync = { ...dataToSync, dataColeta: Timestamp.fromDate(new Date(write.data.dataColeta)) };
                            }

                            if (write.type === 'update' && write.docId) {
                                await App.data.updateDocument(write.collection, write.docId, dataToSync);
                            } else {
                                await App.data.setDocument(write.collection, write.id, dataToSync);
                            }

                            logEntry.items.push({
                                status: 'success', collection: write.collection, data: write.data, error: null
                            });
                            successfulWrites++;
                            successfulKeys.push(key);

                        } catch (error) {
                            if (error.message === 'Item de sincronização offline malformado ou inválido.') {
                                console.error('Item malformado encontrado e descartado:', { write, error });
                                logEntry.items.push({
                                    status: 'failure',
                                    collection: 'malformed',
                                    data: write || 'empty',
                                    error: error.message
                                });
                                unrecoverableKeys.push(key); // Adiciona à lista de descarte
                                discardedWrites++;
                            } else {
                                console.error(`Falha ao sincronizar o item (será tentado novamente):`, { write, error });
                                logEntry.items.push({
                                    status: 'failure',
                                    collection: write?.collection || 'unknown',
                                    data: write?.data || write,
                                    error: error.message || 'Erro desconhecido'
                                });
                                failedWrites++; // Erro recuperável
                            }
                        }
                    }

                    // Etapa 3: Apagar todos os registos sincronizados com sucesso E os irrecuperáveis
                    const keysToDelete = [...successfulKeys, ...unrecoverableKeys];
                    if (keysToDelete.length > 0) {
                        const deleteTx = db.transaction('offline-writes', 'readwrite');
                        for (const key of keysToDelete) {
                            deleteTx.store.delete(key);
                        }
                        await deleteTx.done;
                        console.log(`${keysToDelete.length} registos offline (sincronizados ou corrompidos) foram apagados.`);
                    }

                    // Etapa 4: Registar o resultado da sincronização com lógica melhorada
                    if (logEntry.items.length > 0) {
                        const parts = [];
                        if (successfulWrites > 0) parts.push(`${successfulWrites} registos enviados com sucesso`);
                        if (failedWrites > 0) parts.push(`${failedWrites} falharam e serão tentados novamente`);
                        if (discardedWrites > 0) parts.push(`${discardedWrites} estavam corrompidos e foram descartados`);

                        logEntry.details = parts.join('. ') + '.';

                        if (failedWrites > 0) {
                            logEntry.status = 'failure'; // Se algo falhou e precisa de nova tentativa, o status geral é de falha.
                        } else if (discardedWrites > 0) {
                            logEntry.status = 'partial'; // Se não há falhas de rede, mas algo foi descartado, é parcial.
                        } else {
                            logEntry.status = 'success';
                        }

                        const permanentLogEntry = { ...logEntry, timestamp: serverTimestamp() };
                        const logDocRef = await App.data.addDocument('sync_history_store', permanentLogEntry);

                        App.ui.showSystemNotification(`Sincronização: ${logEntry.status}`, logEntry.details, logEntry.status, { logId: logDocRef.id });
                    }

                } catch (error) {
                    console.error("Ocorreu um erro crítico durante a sincronização:", error);
                    App.ui.showSystemNotification("Erro de Sincronização", "Ocorreu um erro crítico durante o processo. Verifique a consola.", "critical_error");
                } finally {
                    App.state.isSyncing = false;
                    console.log("Processo de sincronização finalizado.");
                }
            },

        async syncGpsLocations() {
            const locationsToSync = await OfflineDB.getAll('gps-locations');
            if (locationsToSync.length === 0) {
                return;
            }

            try {
                const response = await fetch(`${App.config.backendUrl}/api/track/batch`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ locations: locationsToSync }),
                });

                if (response.ok) {
                    const db = await OfflineDB.dbPromise;
                    const tx = db.transaction('gps-locations', 'readwrite');
                    await tx.store.clear();
                    await tx.done;
                    console.log(`${locationsToSync.length} localizações GPS offline foram sincronizadas e limpas.`);
                } else {
                    console.error("Falha ao sincronizar localizações GPS em lote.");
                }
            } catch (error) {
                console.error("Erro de rede ao sincronizar localizações GPS:", error);
            }
        },

            async checkForDraft() {
                const userId = App.state.currentUser.uid;
                try {
                    // Rascunho do plano de colheita (Firestore)
                    const harvestPlanDraft = await App.data.getDocument('userDrafts', userId);
                    if (harvestPlanDraft) {
                        App.state.activeHarvestPlan = harvestPlanDraft;
                        App.ui.showTab('planejamentoColheita');
                        App.ui.showHarvestPlanEditor();

                        const { frontName, startDate, dailyRate } = App.elements.harvest;
                        frontName.value = App.state.activeHarvestPlan.frontName;
                        startDate.value = App.state.activeHarvestPlan.startDate;
                        dailyRate.value = App.state.activeHarvestPlan.dailyRate;

                        App.ui.renderHarvestSequence();
                        return true; // Indica que um rascunho foi restaurado, impedindo a navegação para a última aba
                    }
                } catch (error) {
                    console.error("Erro ao verificar o rascunho do plano de colheita:", error);
                }

                // Rascunhos de formulários de lançamento (LocalStorage)
                ['broca', 'perda', 'cigarrinha'].forEach(formType => {
                    this.loadFormDraft(formType);
                });

                return false; // Permite que a navegação para a última aba prossiga se nenhum rascunho de colheita for encontrado
            },

            saveFormDraft(formType) {
                const form = App.elements[formType]?.form;
                if (!form) return;

                const formData = {};
                form.querySelectorAll('input, select, textarea').forEach(el => {
                    if (el.id) {
                        formData[el.id] = el.type === 'checkbox' ? el.checked : el.value;
                    }
                });
                localStorage.setItem(`draft_${formType}`, JSON.stringify(formData));
            },

            loadFormDraft(formType) {
                const draftData = localStorage.getItem(`draft_${formType}`);
                if (!draftData) return;

                const form = App.elements[formType]?.form;
                if (!form) return;

                App.ui.showConfirmationModal(
                    `Encontramos um rascunho não salvo para o lançamento de ${formType}. Deseja restaurá-lo?`,
                    () => {
                        const formData = JSON.parse(draftData);
                        Object.keys(formData).forEach(id => {
                            const el = document.getElementById(id);
                            if (el) {
                                if (el.type === 'checkbox') {
                                    el.checked = formData[id];
                                } else {
                                    el.value = formData[id];
                                }
                                // Dispara o evento de input para atualizar cálculos e outros listeners
                                el.dispatchEvent(new Event('input'));
                            }
                        });
                        App.ui.showAlert("Rascunho restaurado.", "success");
                    },
                    false // No input needed
                );
            },

            clearFormDraft(formType) {
                localStorage.removeItem(`draft_${formType}`);
            },

            async startGpsTracking() {
                if (App.state.isTracking) return;

                const savePosition = async (position) => {
                    if (position && App.state.currentUser) {
                        const { latitude, longitude } = position.coords;
                        const locationData = {
                            userId: App.state.currentUser.uid,
                            latitude,
                            longitude,
                            companyId: App.state.currentUser.companyId,
                            timestamp: new Date(position.timestamp).toISOString()
                        };
                        try {
                            await OfflineDB.add('gps-locations', locationData);
                        } catch (dbError) {
                            console.error("Falha ao guardar localização GPS no IndexedDB:", dbError);
                        }
                    }
                };

                if (window.Capacitor && Capacitor.isNativePlatform()) {
                    try {
                        const { Geolocation } = Capacitor.Plugins;
                        const permissions = await Geolocation.requestPermissions();
                        if (permissions.location !== 'granted') {
                            console.warn("Permissão de localização do Capacitor não concedida.");
                            return;
                        }

                        App.state.isTracking = true;
                        App.state.locationWatchId = await Geolocation.watchPosition({ enableHighAccuracy: true }, (position, err) => {
                            if (err) {
                                console.warn("Erro no rastreamento de localização do Capacitor:", err.message);
                                return;
                            }
                            savePosition(position);
                        });
                        console.log("Rastreamento de localização (Capacitor) iniciado.");
                    } catch (e) {
                        console.error("Falha ao iniciar o rastreamento de localização do Capacitor:", e);
                    }
                } else if ('geolocation' in navigator) {
                    navigator.geolocation.getCurrentPosition(
                        () => {
                            App.state.isTracking = true;
                            App.state.locationWatchId = navigator.geolocation.watchPosition(
                                (position) => {
                                    savePosition(position);
                                },
                                (err) => {
                                    console.warn("Erro no rastreamento de localização (Web):", err.message);
                                },
                                { enableHighAccuracy: true }
                            );
                            console.log("Rastreamento de localização (Web) iniciado.");
                        },
                        (error) => {
                            if (error.code === error.PERMISSION_DENIED) {
                                console.warn("Permissão de localização (Web) não concedida.");
                                App.ui.showAlert("Para rastreamento de localização, por favor, ative os serviços de localização no seu navegador.", "info");
                            } else {
                                console.error("Falha ao obter permissão de localização (Web):", error.message);
                            }
                        }
                    );
                } else {
                    console.error("Geolocalização não é suportada neste navegador.");
                }
            },

            stopGpsTracking() {
                if (!App.state.isTracking) return;

                if (window.Capacitor && Capacitor.isNativePlatform()) {
                    if (App.state.locationWatchId) {
                        Capacitor.Plugins.Geolocation.clearWatch({ id: App.state.locationWatchId });
                        App.state.locationWatchId = null;
                    }
                } else { // Web Geolocation
                    if (App.state.locationWatchId) {
                        navigator.geolocation.clearWatch(App.state.locationWatchId);
                        App.state.locationWatchId = null;
                    }
                }

                if (App.state.locationUpdateIntervalId) {
                    clearInterval(App.state.locationUpdateIntervalId);
                    App.state.locationUpdateIntervalId = null;
                }

                App.state.isTracking = false;
                console.log("Rastreamento de localização parado.");
            },

            async viewHistory() {
                const { userSelect, startDate, endDate } = App.elements.historyFilterModal;
                const userId = userSelect.value;
                const start = startDate.value;
                const end = endDate.value;

                if (!userId || !start || !end) {
                    App.ui.showAlert("Por favor, selecione um utilizador e um intervalo de datas.", "warning");
                    return;
                }

                const map = App.state.mapboxMap;
                if (!map) {
                    App.ui.showAlert("O mapa principal não foi inicializado.", "error");
                    return;
                }

                this.clearHistory(); // Limpa a rota anterior antes de desenhar uma nova
                App.ui.hideHistoryFilterModal();
                App.ui.setLoading(true, "A buscar histórico...");

                try {
                    const companyId = App.state.currentUser.companyId;
                    const response = await fetch(`${App.config.backendUrl}/api/history?userId=${userId}&startDate=${start}&endDate=${end}&companyId=${companyId}`);
                    if (!response.ok) {
                        const errorData = await response.json();
                        throw new Error(errorData.message || `Erro do servidor: ${response.status}`);
                    }
                    const historyData = await response.json();

                    if (historyData.length === 0) {
                        App.ui.showAlert("Nenhum dado de localização encontrado para este período.", "info");
                        return;
                    }

                    const coordinates = historyData.map(p => [p.longitude, p.latitude]);

                    map.addSource('history-route', {
                        'type': 'geojson',
                        'data': { 'type': 'Feature', 'geometry': { 'type': 'LineString', 'coordinates': coordinates } }
                    });

                    const pointsGeoJSON = {
                        type: 'FeatureCollection',
                        features: historyData.map((p, i) => ({
                            type: 'Feature',
                            geometry: { type: 'Point', coordinates: [p.longitude, p.latitude] },
                            properties: {
                                isStart: i === 0,
                                isEnd: i === historyData.length - 1,
                                timestamp: new Date(p.timestamp).toLocaleString('pt-BR')
                            }
                        }))
                    };

                    map.addSource('history-points', { type: 'geojson', data: pointsGeoJSON });

                    map.addLayer({
                        'id': 'history-route',
                        'type': 'line',
                        'source': 'history-route',
                        'layout': { 'line-join': 'round', 'line-cap': 'round' },
                        'paint': { 'line-color': '#FFD700', 'line-width': 5, 'line-opacity': 0.8 }
                    });

                    map.addLayer({
                        id: 'history-points',
                        type: 'circle',
                        source: 'history-points',
                        paint: {
                            'circle-radius': 8,
                            'circle-color': [
                                'case',
                                ['boolean', ['get', 'isStart'], false], '#388e3c',
                                ['boolean', ['get', 'isEnd'], false], '#d32f2f',
                                '#FFFFFF'
                            ],
                            'circle-stroke-width': 2,
                            'circle-stroke-color': '#000000'
                        }
                    });

                    const bounds = new mapboxgl.LngLatBounds(coordinates[0], coordinates[0]);
                    for (const coord of coordinates) { bounds.extend(coord); }
                    map.fitBounds(bounds, { padding: 80, duration: 1000 });

                } catch (error) {
                    App.ui.showAlert(`Erro ao buscar histórico: ${error.message}`, 'error');
                } finally {
                    App.ui.setLoading(false);
                }
            },

            clearHistory() {
                const map = App.state.mapboxMap;
                if (!map) return;

                const layers = ['history-points', 'history-route'];
                const sources = ['history-points', 'history-route'];

                layers.forEach(layerId => {
                    if (map.getLayer(layerId)) {
                        map.removeLayer(layerId);
                    }
                });

                sources.forEach(sourceId => {
                    if (map.getSource(sourceId)) {
                        map.removeSource(sourceId);
                    }
                });
                 App.ui.showAlert("Rota do histórico limpa.", "info");
            },

            async getAtrPrediction() {
                const { fazenda: fazendaSelect, atr: atrInput } = App.elements.harvest;
                const atrSpinner = document.getElementById('atr-spinner');

                atrInput.value = '';
                atrInput.readOnly = true; // Impede a digitação durante o cálculo
                atrInput.placeholder = 'Calculando...';

                const farmId = fazendaSelect.value;

                if (!farmId) {
                    atrInput.placeholder = 'ATR Previsto';
                    atrInput.readOnly = false;
                    return;
                }

                const farm = App.state.fazendas.find(f => f.id === farmId);
                if (!farm) {
                    atrInput.readOnly = false;
                    return;
                }

                if(atrSpinner) atrSpinner.style.display = 'inline-block';

                try {
                    const response = await fetch(`${App.config.backendUrl}/api/calculate-atr`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            codigoFazenda: farm.code,
                            companyId: App.state.currentUser.companyId
                        }),
                    });

                    if (!response.ok) {
                        const errorData = await response.json();
                        throw new Error(errorData.message || `Erro do servidor: ${response.status}`);
                    }
                    const result = await response.json();

                    if (result && typeof result.predicted_atr === 'number') {
                        if (result.predicted_atr > 0) {
                            atrInput.value = result.predicted_atr.toFixed(2);
                            atrInput.placeholder = 'ATR Previsto';
                        } else {
                            atrInput.placeholder = 'Sem histórico';
                            App.ui.showAlert('Nenhum histórico de ATR encontrado para esta fazenda.', 'info');
                        }
                    } else {
                         atrInput.placeholder = 'Sem histórico';
                    }
                } catch (error) {
                    console.error("Erro ao buscar ATR previsto:", error);
                    App.ui.showAlert(`Não foi possível calcular o ATR: ${error.message}`, 'error');
                    atrInput.placeholder = 'Erro ao calcular';
                } finally {
                    if(atrSpinner) atrSpinner.style.display = 'none';
                }
            },

            impersonateCompany(companyId) {
                if (App.state.currentUser.role !== 'super-admin' || App.state.isImpersonating) {
                    return;
                }

                const companyToImpersonate = App.state.companies.find(c => c.id === companyId);
                if (!companyToImpersonate) {
                    App.ui.showAlert("Empresa não encontrada.", "error");
                    return;
                }

                // Store original user
                App.state.originalUser = { ...App.state.currentUser };
                App.state.isImpersonating = true;

                // Create a fake admin user for the target company
                const adminPermissions = App.config.roles['admin'];
                App.state.currentUser = {
                    ...App.state.originalUser, // Keep some original info like UID, email
                    role: 'admin',
                    permissions: adminPermissions,
                    companyId: companyId,
                };

                // Re-initialize the app view
                App.ui.showImpersonationBanner(companyToImpersonate.name);
                App.data.listenToAllData(); // This will now use the impersonated companyId
                App.ui.renderMenu();
                App.ui.showTab('dashboard');
            },

            stopImpersonating() {
                if (!App.state.isImpersonating || !App.state.originalUser) {
                    return;
                }

                // Restore original user
                App.state.currentUser = { ...App.state.originalUser };
                App.state.originalUser = null;
                App.state.isImpersonating = false;

                // Re-initialize the app view
                App.data.listenToAllData(); // This will go back to super-admin view
                App.ui.renderMenu();
                App.ui.showTab('gerenciarEmpresas'); // Go back to a sensible super-admin tab
                App.ui.hideImpersonationBanner();
            },

            async saveGlobalFeatures() {
                const grid = document.getElementById('globalFeaturesGrid');
                if (!grid) {
                    App.ui.showAlert("Elemento de controlo de features não encontrado.", "error");
                    return;
                }

                // Salva o estado antigo para comparação
                const oldGlobalConfigs = { ...App.state.globalConfigs };

                const newGlobalConfigs = {};
                grid.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                    newGlobalConfigs[cb.dataset.feature] = cb.checked;
                });

                App.ui.setLoading(true, "A guardar e notificar...");
                try {
                    await App.data.setDocument('global_configs', 'main', newGlobalConfigs);
                    App.ui.showAlert("Configurações globais guardadas com sucesso!", "success");

                    // Lógica de notificação
                    await this.notifyAdminsOfNewFeatures(oldGlobalConfigs, newGlobalConfigs);

                } catch (error) {
                    App.ui.showAlert("Erro ao guardar as configurações globais.", "error");
                    console.error("Erro ao guardar configurações globais:", error);
                } finally {
                    App.ui.setLoading(false);
                }
            },

            async notifyAdminsOfNewFeatures(oldConfigs, newConfigs) {
                const newlyEnabledFeatures = Object.keys(newConfigs).filter(key => newConfigs[key] && !oldConfigs[key]);

                if (newlyEnabledFeatures.length === 0) {
                    console.log("Nenhuma nova feature ativada. Nenhuma notificação a ser enviada.");
                    return;
                }

                console.log("Features recém-ativadas:", newlyEnabledFeatures);

                try {
                    // 1. Lidar com a limitação de 10 elementos do Firestore para 'array-contains-any'
                    const chunkArray = (array, size) => {
                        const chunks = [];
                        for (let i = 0; i < array.length; i += size) {
                            chunks.push(array.slice(i, i + size));
                        }
                        return chunks;
                    };

                    const featureChunks = chunkArray(newlyEnabledFeatures, 10);
                    const queryPromises = featureChunks.map(chunk => {
                        const q = query(collection(db, 'companies'), where('subscribedModules', 'array-contains-any', chunk));
                        return getDocs(q);
                    });

                    const allSnapshots = await Promise.all(queryPromises);

                    // Consolidar e remover duplicatas
                    const companyMap = new Map();
                    allSnapshots.forEach(snapshot => {
                        snapshot.docs.forEach(doc => {
                            if (!companyMap.has(doc.id)) {
                                companyMap.set(doc.id, { id: doc.id, ...doc.data() });
                            }
                        });
                    });
                    const relevantCompanies = Array.from(companyMap.values());


                    if (relevantCompanies.length === 0) {
                        console.log("Nenhuma empresa encontrada que subscreva aos módulos recém-ativados.");
                        return;
                    }

                    const batch = writeBatch(db);
                    let notificationCount = 0;

                    // 2. Para cada empresa, encontrar os seus administradores
                    for (const company of relevantCompanies) {
                        const adminsQuery = query(collection(db, 'users'), where('companyId', '==', company.id), where('role', '==', 'admin'));
                        const adminsSnapshot = await getDocs(adminsQuery);

                        if (adminsSnapshot.empty) continue;

                        // 3. Descobrir quais features são novas para esta empresa específica
                        const newFeaturesForThisCompany = newlyEnabledFeatures.filter(feature =>
                            (company.subscribedModules || []).includes(feature)
                        );

                        if (newFeaturesForThisCompany.length === 0) continue;

                        const featureLabels = newFeaturesForThisCompany.map(key => {
                            const menuItem = App.config.menuConfig.flatMap(item => item.submenu || [item]).find(i => i.permission === key);
                            return menuItem ? menuItem.label : key;
                        }).join(', ');

                        const message = `Novas funcionalidades estão disponíveis para a sua empresa: ${featureLabels}. Visite a secção correspondente para explorar.`;

                        // 4. Criar uma notificação para cada administrador
                        adminsSnapshot.forEach(adminDoc => {
                            const notificationRef = doc(collection(db, 'notifications')); // Cria uma nova notificação com ID automático
                            batch.set(notificationRef, {
                                userId: adminDoc.id,
                                companyId: company.id,
                                title: "Nova Funcionalidade Ativada!",
                                message: message,
                                type: 'info',
                                timestamp: serverTimestamp(),
                                read: false
                            });
                            notificationCount++;
                        });
                    }

                    if (notificationCount > 0) {
                        await batch.commit();
                        console.log(`${notificationCount} notificações enviadas para administradores.`);
                        App.ui.showAlert(`${notificationCount} administradores de empresas foram notificados sobre as novas funcionalidades.`, "info", 5000);
                    }

                } catch (error) {
                    console.error("Erro ao notificar administradores sobre novas features:", error);
                    App.ui.showAlert("Ocorreu um erro ao tentar notificar os administradores.", "error");
                }
            },

            async deduplicateTraps() {
                const confirmationMessage = `Tem a certeza que deseja remover as armadilhas duplicadas? Esta ação irá manter apenas a armadilha mais recente em cada talhão e apagar todas as outras. A ação não pode ser desfeita.`;
                App.ui.showConfirmationModal(confirmationMessage, async () => {
                    App.ui.setLoading(true, "A analisar armadilhas duplicadas...");
                    try {
                        const allTraps = await App.actions.getConsolidatedData('armadilhas');
                        const trapsByPlot = new Map();

                        // Agrupar armadilhas por fazenda e talhão
                        allTraps.forEach(trap => {
                            const key = `${trap.fazendaNome}-${trap.talhaoNome}`;
                            if (!trapsByPlot.has(key)) {
                                trapsByPlot.set(key, []);
                            }
                            trapsByPlot.get(key).push(trap);
                        });

                        const batch = writeBatch(db);
                        let trapsToDeleteCount = 0;

                        for (const [key, traps] of trapsByPlot.entries()) {
                            if (traps.length > 1) {
                                // Ordenar para encontrar a mais recente
                                traps.sort((a, b) => {
                                    const dateA = a.dataInstalacao?.toDate ? a.dataInstalacao.toDate() : new Date(a.dataInstalacao);
                                    const dateB = b.dataInstalacao?.toDate ? b.dataInstalacao.toDate() : new Date(b.dataInstalacao);
                                    return dateB - dateA;
                                });

                                const trapToKeep = traps[0];
                                const trapsToDelete = traps.slice(1);

                                trapsToDelete.forEach(trap => {
                                    if (trap.id.startsWith('offline_')) {
                                        console.warn("A tentar apagar uma armadilha duplicada que ainda está offline. Esta ação será ignorada.", trap);
                                    } else {
                                        const docRef = doc(db, 'armadilhas', trap.id);
                                        batch.delete(docRef);
                                        trapsToDeleteCount++;
                                    }
                                });
                            }
                        }

                        if (trapsToDeleteCount > 0) {
                            App.ui.setLoading(true, `A apagar ${trapsToDeleteCount} armadilhas duplicadas...`);
                            await batch.commit();
                            App.ui.showAlert(`${trapsToDeleteCount} armadilhas duplicadas foram removidas com sucesso!`, 'success');
                        } else {
                            App.ui.showAlert("Nenhuma armadilha duplicada foi encontrada.", 'info');
                        }
                    } catch (error) {
                        console.error("Erro ao remover armadilhas duplicadas:", error);
                        App.ui.showAlert(`Ocorreu um erro: ${error.message}`, 'error');
                    } finally {
                        App.ui.setLoading(false);
                    }
                });
            },


            setupPlantingGoals() {
                const container = document.getElementById('planting-goals-container');
                if (!container) return;

                const cultures = ['CANADEACUCAR', 'SOJA', 'MILHO', 'ALGODAO', 'SORGO'];
                const currentGoals = App.state.companyConfig.plantingGoals || {};
                container.innerHTML = ''; // Limpa o container

                const goalsGrid = document.createElement('div');
                goalsGrid.className = 'permission-grid'; // Reutiliza um estilo de grid existente
                goalsGrid.style.gridTemplateColumns = 'repeat(auto-fill, minmax(300px, 1fr))';
                goalsGrid.style.gap = '20px';

                cultures.forEach(culture => {
                    const goalData = currentGoals[culture] || { total: '', daily: '' };
                    const formattedCulture = culture.replace(/_/g, ' ').charAt(0).toUpperCase() + culture.slice(1).toLowerCase();

                    const cultureCard = document.createElement('div');
                    cultureCard.className = 'form-col'; // Um card simples para agrupar os inputs
                    cultureCard.style.border = '1px solid var(--color-border)';
                    cultureCard.style.borderRadius = '8px';
                    cultureCard.style.padding = '15px';

                    cultureCard.innerHTML = `
                        <h4 style="margin-top: 0; margin-bottom: 15px; color: var(--color-primary);">${formattedCulture}</h4>
                        <div class="form-row">
                            <div class="form-col">
                                <label for="goal-total-${culture}">Meta Total (ha)</label>
                                <input type="number" id="goal-total-${culture}" data-culture="${culture}" data-type="total" value="${goalData.total}" placeholder="0 ha">
                            </div>
                            <div class="form-col">
                                <label for="goal-daily-${culture}">Meta Diária (ha)</label>
                                <input type="number" id="goal-daily-${culture}" data-culture="${culture}" data-type="daily" value="${goalData.daily}" placeholder="0 ha/dia">
                            </div>
                        </div>
                    `;
                    goalsGrid.appendChild(cultureCard);
                });

                container.appendChild(goalsGrid);
            },

            async savePlantingGoals() {
                const container = document.getElementById('planting-goals-container');
                if (!container) return;

                const newGoals = {};
                const cultures = ['CANADEACUCAR', 'SOJA', 'MILHO', 'ALGODAO', 'SORGO'];
                cultures.forEach(culture => {
                    const totalInput = container.querySelector(`input[data-culture="${culture}"][data-type="total"]`);
                    const dailyInput = container.querySelector(`input[data-culture="${culture}"][data-type="daily"]`);

                    const totalValue = totalInput ? parseFloat(totalInput.value) : 0;
                    const dailyValue = dailyInput ? parseFloat(dailyInput.value) : 0;

                    // Only add the culture to the goals object if there's a valid value (including 0)
                    if (!isNaN(totalValue) || !isNaN(dailyValue)) {
                        newGoals[culture] = {
                            total: !isNaN(totalValue) ? Math.max(0, totalValue) : 0,
                            daily: !isNaN(dailyValue) ? Math.max(0, dailyValue) : 0
                        };
                    }
                });

                try {
                    // By constructing a new object and setting it, we overwrite the old one.
                    // The setDocument function with merge:true will replace the plantingGoals field entirely.
                    await App.data.setDocument('config', App.state.currentUser.companyId, { plantingGoals: newGoals }, { merge: true });

                    // ATUALIZAÇÃO IMEDIATA DO ESTADO LOCAL
                    App.state.companyConfig.plantingGoals = newGoals;

                    App.ui.showAlert("Metas de plantio guardadas com sucesso!", "success");
                } catch (error) {
                    App.ui.showAlert("Erro ao guardar as metas de plantio.", "error");
                    console.error("Error saving planting goals:", error);
                }
            },

            startAutoSync() {
                if (App.state.syncInterval) {
                    console.log("A sincronização automática já está ativa.");
                    return;
                }
                // Sincroniza a cada 1 hora
                const umaHora = 60 * 60 * 1000;
                App.state.syncInterval = setInterval(() => {
                    if (navigator.onLine) {
                        console.log("Sincronização automática em primeiro plano iniciada...");
                        App.actions.syncOfflineWrites();
                    } else {
                        console.log("Sincronização automática ignorada (offline).");
                    }
                }, umaHora);
                console.log("Sincronização automática em primeiro plano configurada para cada 1 hora.");
            },

            stopAutoSync() {
                if (App.state.syncInterval) {
                    clearInterval(App.state.syncInterval);
                    App.state.syncInterval = null;
                    console.log("Sincronização automática em primeiro plano parada.");
                }
            },

            async enableOfflineLogin() {
                const passwordInput = document.getElementById('enableOfflinePassword');
                const password = passwordInput.value;
                const currentUser = App.state.currentUser;

                if (!password) {
                    App.ui.showAlert("Por favor, insira a sua senha atual para confirmar.", "error");
                    return;
                }

                if (!navigator.onLine) {
                    App.ui.showAlert("É preciso estar online para habilitar o login offline pela primeira vez.", "warning");
                    return;
                }

                App.ui.setLoading(true, "A verificar senha e a guardar credenciais...");

                try {
                    // 1. Re-autenticar para verificar a senha
                    const user = auth.currentUser;
                    const credential = EmailAuthProvider.credential(user.email, password);
                    await reauthenticateWithCredential(user, credential);

                    // 2. Gerar "salt" e "hash" da senha
                    const salt = CryptoJS.lib.WordArray.random(128 / 8).toString();
                    const hashedPassword = CryptoJS.PBKDF2(password, salt, {
                        keySize: 256 / 32,
                        iterations: 1000
                    }).toString();

                    // 3. Preparar os dados para guardar
                    const userProfileToSave = {
                        uid: currentUser.uid,
                        email: currentUser.email,
                        username: currentUser.username,
                        role: currentUser.role,
                        permissions: currentUser.permissions,
                        companyId: currentUser.companyId,
                    };
                    const credentialsToStore = {
                        email: currentUser.email.toLowerCase(),
                        hashedPassword: hashedPassword,
                        salt: salt,
                        userProfile: userProfileToSave
                    };

                    // 4. Guardar no IndexedDB
                    await OfflineDB.set('offline-credentials', credentialsToStore);

                    App.ui.showAlert("Login offline habilitado/atualizado com sucesso!", "success");
                    App.ui.closeEnableOfflineLoginModal();

                } catch (error) {
                    if (error.code === 'auth/wrong-password' || error.code === 'auth/invalid-credential' || error.code === 'auth/invalid-login-credentials') {
                        App.ui.showAlert("A senha está incorreta.", "error");
                    } else {
                        App.ui.showAlert("Ocorreu um erro ao habilitar o login offline.", "error");
                        console.error("Erro ao habilitar login offline:", error);
                    }
                } finally {
                    App.ui.setLoading(false);
                    passwordInput.value = '';
                }
            },
        },
        gemini: {
            async _callGeminiAPI(prompt, contextData, loadingMessage = "A processar com IA...") {
                App.ui.setLoading(true, loadingMessage);
                try {
                    const response = await fetch(`${App.config.backendUrl}/api/gemini/generate`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            prompt,
                            contextData,
                            companyId: App.state.currentUser.companyId
                        }),
                    });

                    if (!response.ok) {
                        const errorData = await response.json();
                        throw new Error(errorData.message || `Erro do servidor: ${response.status}`);
                    }
                    return await response.json();
                } catch (error) {
                    App.ui.showAlert(`Erro ao comunicar com a IA: ${error.message}`, 'error');
                    console.error("Erro na chamada da API Gemini:", error);
                    return null;
                } finally {
                    App.ui.setLoading(false);
                }
            },

            async getOptimizedHarvestSequence() {
                const plan = App.state.activeHarvestPlan;
                if (!plan || plan.sequence.length === 0) {
                    App.ui.showAlert("Adicione fazendas à sequência antes de otimizar.", "warning");
                    return;
                }

                const prompt = `
                    Otimize a seguinte sequência de colheita de cana-de-açúcar para o mês de ${new Date(plan.startDate).toLocaleString('pt-BR', { month: 'long' })}.
                    Sua tarefa é ser um especialista em agronomia e otimizar a logística de colheita.
                    
                    Critérios de otimização, em ordem de importância:
                    1.  **Potencial de Açúcar (ATR):** Priorize o ATR mais alto. É o fator mais importante.
                    2.  **Maturador:** Se houver maturador aplicado, a colheita ideal é entre 15 e 30 dias após a aplicação. Priorize talhões nessa janela.
                    3.  **Variedade vs. Mês:** Considere a época ideal de colheita para cada variedade. Variedades de início de safra devem ser colhidas mais cedo (Abril, Maio), as de meio no meio, e as de fim de safra mais tarde (Setembro, Outubro). Use seu conhecimento para julgar.
                    4.  **Idade da Cana:** Cana mais velha (maior idade em meses) geralmente deve ser priorizada, mas os critérios acima são mais importantes.
                    5.  **Proximidade na Sequência Original:** Se todos os outros critérios forem semelhantes, tente manter a ordem original para não atrapalhar a logística.

                    Analise os dados de cada grupo e retorne um array JSON contendo APENAS os IDs dos grupos na ordem otimizada. O array deve se chamar "optimizedSequence".
                    Exemplo de Resposta: { "optimizedSequence": [1678886400000, 1678886500000, ...] }
                `;

                const contextData = plan.sequence.map((group, index) => {
                    const farm = App.state.fazendas.find(f => f.code === group.fazendaCodigo);
                    const varieties = new Set();
                    group.plots.forEach(plot => {
                        const talhao = farm?.talhoes.find(t => t.id === plot.talhaoId);
                        if (talhao?.variedade) {
                            varieties.add(talhao.variedade);
                        }
                    });

                    return {
                        id: group.id,
                        fazendaName: group.fazendaName,
                        varieties: Array.from(varieties),
                        originalOrder: index + 1,
                        atr: group.atr,
                        averageAgeMonths: App.actions.calculateAverageAge(group, new Date(plan.startDate)),
                        maturadorDays: App.actions.calculateMaturadorDays(group)
                    };
                });

                const result = await this._callGeminiAPI(prompt, contextData, "A otimizar sequência com IA...");

                if (result && result.optimizedSequence && Array.isArray(result.optimizedSequence)) {
                    const optimizedIds = result.optimizedSequence;
                    const newSequence = [];
                    const groupMap = new Map(plan.sequence.map(g => [g.id, g]));

                    optimizedIds.forEach(id => {
                        if (groupMap.has(id)) {
                            newSequence.push(groupMap.get(id));
                            groupMap.delete(id);
                        }
                    });

                    // Adiciona quaisquer grupos que a IA possa ter esquecido no final
                    groupMap.forEach(group => newSequence.push(group));

                    plan.sequence = newSequence;
                    App.ui.renderHarvestSequence();
                    App.ui.showAlert("Sequência de colheita otimizada pela IA!", "success");
                } else {
                    App.ui.showAlert("A IA não conseguiu otimizar a sequência ou retornou um formato inválido.", "error");
                }
            },

            async getPlanningSuggestions() {
                const pendingPlans = App.state.planos.filter(p => p.status === 'Pendente');
                if (pendingPlans.length === 0) {
                    App.ui.showAlert("Não há inspeções pendentes para analisar.", "info");
                    return;
                }

                const prompt = `
                    Com base na lista de inspeções de broca e perdas pendentes, sugira uma ordem de prioridade.
                    Critérios de prioridade:
                    1. Atraso: Inspeções com data prevista no passado são mais urgentes.
                    2. Histórico: Fazendas com histórico de problemas (se disponível no contexto) devem ser priorizadas.
                    3. Tipo: Inspeções de broca podem ser mais críticas se houver um surto conhecido.

                    Retorne um JSON com duas chaves: "analysis" (uma breve análise em texto sobre a sugestão) e "priority" (um array com os IDs dos planos na ordem de prioridade).
                    Exemplo: { "analysis": "A inspeção na Fazenda X está atrasada e deve ser feita primeiro...", "priority": ["id_plano_1", "id_plano_2", ...] }
                `;

                const contextData = pendingPlans.map(p => ({
                    id: p.id,
                    fazenda: p.fazendaCodigo,
                    talhao: p.talhao,
                    tipo: p.tipo,
                    dataPrevista: p.dataPrevista,
                    responsavel: p.usuarioResponsavel
                }));

                const result = await this._callGeminiAPI(prompt, contextData, "A obter sugestões da IA...");

                if (result && result.analysis && result.priority) {
                    const reorderedPlans = [...App.state.planos];
                    const priorityMap = new Map(result.priority.map((id, index) => [id, index]));

                    reorderedPlans.sort((a, b) => {
                        const priorityA = priorityMap.has(a.id) ? priorityMap.get(a.id) : Infinity;
                        const priorityB = priorityMap.has(b.id) ? priorityMap.get(b.id) : Infinity;
                        return priorityA - priorityB;
                    });

                    App.state.planos = reorderedPlans;
                    App.ui.renderPlanejamento();

                    App.ui.showConfirmationModal(
                        result.analysis,
                        () => {}, // Apenas para mostrar a informação
                        false
                    );
                    const modal = App.elements.confirmationModal;
                    modal.title.textContent = "Sugestão da AgroVetor AI";
                    modal.confirmBtn.textContent = "OK";
                    modal.cancelBtn.style.display = 'none';

                } else {
                    App.ui.showAlert("A IA não conseguiu gerar sugestões ou retornou um formato inválido.", "error");
                }
            },

        },

        mapModule: {
            initMap() {
                if (App.state.mapboxMap) return; // Evita reinicialização
                if (typeof mapboxgl === 'undefined') {
                    console.error("Mapbox GL JS não está carregado.");
                    App.ui.showAlert("Erro ao carregar a biblioteca do mapa.", "error");
                    return;
                }

                try {
                    mapboxgl.accessToken = 'pk.eyJ1IjoiY2FybG9zaGduIiwiYSI6ImNtZDk0bXVxeTA0MTcyam9sb2h1dDhxaG8ifQ.uf0av4a0WQ9sxM1RcFYT2w';
                    const mapContainer = App.elements.monitoramentoAereo.mapContainer;

                    App.state.mapboxMap = new mapboxgl.Map({
                        container: mapContainer,
                        style: 'mapbox://styles/mapbox/satellite-streets-v12', // Estilo satélite com ruas
                        center: [-48.45, -21.17], // [lng, lat]
                        zoom: 12,
                        attributionControl: false
                    });

                    App.state.mapboxMap.on('load', () => {
                        console.log("Mapbox map loaded.");
                        this.watchUserPosition();
                        this.loadShapesOnMap();
                        this.loadTraps();
                    });

                } catch (e) {
                    console.error("Erro ao inicializar o Mapbox:", e);
                    App.ui.showAlert("Não foi possível carregar o mapa.", "error");
                }
            },

            watchUserPosition() {
                if ('geolocation' in navigator) {
                    navigator.geolocation.watchPosition(
                        (position) => {
                            const { latitude, longitude } = position.coords;
                            this.updateUserPosition(latitude, longitude);
                        },
                        (error) => {
                            console.warn(`Erro de Geolocalização: ${error.message}`);
                            App.ui.showAlert("Não foi possível obter sua localização.", "warning");
                        },
                        { enableHighAccuracy: true, timeout: 27000, maximumAge: 60000 }
                    );
                } else {
                    App.ui.showAlert("Geolocalização não é suportada pelo seu navegador.", "error");
                }
            },

            updateUserPosition(lat, lng) {
                const userPosition = [lng, lat]; // Mapbox uses [lng, lat]
                
                if (!App.state.mapboxMap) return;

                if (!App.state.mapboxUserMarker) {
                    const el = document.createElement('div');
                    el.style.backgroundColor = '#4285F4';
                    el.style.width = '16px';
                    el.style.height = '16px';
                    el.style.borderRadius = '50%';
                    el.style.border = '2px solid #ffffff';

                    App.state.mapboxUserMarker = new mapboxgl.Marker(el)
                        .setLngLat(userPosition)
                        .addTo(App.state.mapboxMap);

                    App.state.mapboxMap.flyTo({ center: userPosition, zoom: 15 });
                } else {
                    App.state.mapboxUserMarker.setLngLat(userPosition);
                }
            },

            centerMapOnUser() {
                if (App.state.mapboxUserMarker) {
                    const userPosition = App.state.mapboxUserMarker.getLngLat();
                    App.state.mapboxMap.flyTo({ center: userPosition, zoom: 16 });
                } else {
                    App.ui.showAlert("Ainda não foi possível obter sua localização.", "info");
                }
            },

            async handleShapefileUpload(e) {
                const file = e.target.files[0];
                const input = e.target;
                if (!file) return;

                if (!file.name.toLowerCase().endsWith('.zip')) {
                    App.ui.showAlert("Por favor, selecione um arquivo .zip", "error");
                    input.value = '';
                    return;
                }

                const companyId = App.state.currentUser.companyId;
                if (!companyId) {
                    App.ui.showAlert("ID da empresa não encontrado. Não é possível fazer o upload.", "error");
                    return;
                }

                App.ui.setLoading(true, "A enviar o arquivo para o armazenamento...");

                const storageRef = ref(storage, `shapefiles/${companyId}/map.zip`);

                try {
                    const uploadResult = await uploadBytes(storageRef, file);
                    App.ui.setLoading(true, "A obter o link de download...");

                    const downloadURL = await getDownloadURL(uploadResult.ref);

                    await App.data.setDocument('config', companyId, { shapefileURL: downloadURL }, { merge: true });

                    App.ui.showAlert("Arquivo enviado com sucesso! O mapa será atualizado em breve.", "success");

                } catch (error) {
                    console.error("Erro no upload do shapefile:", error);
                    let errorMessage = "Ocorreu um erro durante o upload.";
                    if (error.code) {
                        switch (error.code) {
                            case 'storage/unauthorized':
                                errorMessage = "Não tem permissão para enviar arquivos. Verifique as regras de segurança do Storage.";
                                break;
                            case 'storage/canceled':
                                errorMessage = "O envio foi cancelado.";
                                break;
                            case 'storage/unknown':
                                errorMessage = "Ocorreu um erro desconhecido no servidor.";
                                break;
                        }
                    }
                    App.ui.showAlert(errorMessage, "error");
                } finally {
                    App.ui.setLoading(false);
                    input.value = '';
                }
            },

            async loadAndCacheShapes(url) {
                const mapContainer = document.getElementById('map-container');
                if (!url) {
                    if (mapContainer) mapContainer.classList.remove('loading');
                    return;
                }
                console.log("Iniciando o carregamento dos contornos do mapa em segundo plano...");
                if (mapContainer) mapContainer.classList.add('loading');
                try {
                    const urlWithCacheBuster = `${url}?t=${new Date().getTime()}`;
                    const response = await fetch(urlWithCacheBuster);
                    if (!response.ok) throw new Error(`Não foi possível baixar o shapefile: ${response.statusText}`);
                    const buffer = await response.arrayBuffer();

                    await OfflineDB.set('shapefile-cache', buffer, 'shapefile-zip');

                    console.log("Processando e desenhando os talhões no mapa...");
                    let geojson = await shp(buffer);

                    // REPROJEÇÃO: Converte as coordenadas da projeção de origem para WGS84
                    if (window.proj4) {
                        const sourceProjection = "EPSG:31982"; // SIRGAS 2000 UTM Zone 22S
                        const destProjection = "WGS84";
                        geojson.features.forEach(feature => {
                            if (feature.geometry && feature.geometry.coordinates) {
                                try {
                                    feature.geometry.coordinates = feature.geometry.coordinates.map(polygon =>
                                        polygon.map(coord => proj4(sourceProjection, destProjection, coord))
                                    );
                                } catch (e) {
                                    console.error("Erro ao reprojetar coordenada:", coord, e);
                                }
                            }
                        });
                        console.log(`Reprojeção de coordenadas de ${sourceProjection} para ${destProjection} concluída.`);
                    }


                    // Normaliza as propriedades para garantir que os rótulos funcionem
                    let featureIdCounter = 0;
                    geojson.features.forEach(feature => {
                        feature.id = featureIdCounter++; // **HOTFIX** Adiciona um ID numérico único
                        const fundo = this._findProp(feature, ['FUNDO_AGR', 'FUNDO_AGRI', 'FUNDOAGRICOLA']);
                        const talhao = this._findProp(feature, ['CD_TALHAO', 'TALHAO', 'COD_TALHAO', 'NAME']);
                        feature.properties.AGV_FUNDO = String(fundo).trim();
                        feature.properties.AGV_TALHAO = String(talhao).trim();
                    });


                    App.state.geoJsonData = geojson;
                    if (App.state.mapboxMap) {
                        this.loadShapesOnMap();
                    }
                    console.log("Contornos do mapa carregados com sucesso.");
                } catch(err) {
                    console.error("Erro ao carregar shapefile do Storage:", err);
                    App.ui.showAlert("Falha ao carregar os desenhos do mapa. Tentando usar o cache.", "warning");
                    if (mapContainer) mapContainer.classList.remove('loading');
                    this.loadOfflineShapes();
                }
            },

            async loadOfflineShapes() {
                const mapContainer = document.getElementById('map-container');
                if (mapContainer) mapContainer.classList.add('loading');
                try {
                    const buffer = await OfflineDB.get('shapefile-cache', 'shapefile-zip');
                    if (buffer) {
                        App.ui.showAlert("A carregar mapa do cache offline.", "info");
                        let geojson = await shp(buffer);

                        // REPROJEÇÃO: Converte as coordenadas da projeção de origem para WGS84
                        if (window.proj4) {
                            const sourceProjection = "EPSG:31982"; // SIRGAS 2000 UTM Zone 22S
                            const destProjection = "WGS84";
                            geojson.features.forEach(feature => {
                                if (feature.geometry && feature.geometry.coordinates) {
                                    try {
                                        feature.geometry.coordinates = feature.geometry.coordinates.map(polygon =>
                                            polygon.map(coord => proj4(sourceProjection, destProjection, coord))
                                        );
                                    } catch (e) {
                                        console.error("Erro ao reprojetar coordenada do cache:", coord, e);
                                    }
                                }
                            });
                            console.log(`Reprojeção de coordenadas do cache de ${sourceProjection} para ${destProjection} concluída.`);
                        }

                        // Normaliza as propriedades também para o cache offline
                        let featureIdCounter = 0;
                        geojson.features.forEach(feature => {
                            feature.id = featureIdCounter++; // **HOTFIX** Adiciona um ID numérico único
                            const fundo = this._findProp(feature, ['FUNDO_AGR', 'FUNDO_AGRI', 'FUNDOAGRICOLA']);
                            const talhao = this._findProp(feature, ['CD_TALHAO', 'TALHAO', 'COD_TALHAO', 'NAME']);
                            feature.properties.AGV_FUNDO = String(fundo).trim();
                            feature.properties.AGV_TALHAO = String(talhao).trim();
                        });

                        App.state.geoJsonData = geojson;
                        if (App.state.mapboxMap) {
                            this.loadShapesOnMap();
                        }
                    }
                } catch (error) {
                    console.error("Erro crítico ao carregar ou processar o mapa offline:", error);
                    App.ui.showAlert("Falha ao carregar os desenhos do mapa offline. O mapa pode não ser exibido, mas o aplicativo continuará a funcionar.", "error", 6000);
                } finally {
                    if (mapContainer) mapContainer.classList.remove('loading');
                }
            },

            loadShapesOnMap() {
                const mapContainer = document.getElementById('map-container');
                if (!App.state.mapboxMap || !App.state.geoJsonData) {
                    if (mapContainer) mapContainer.classList.remove('loading');
                    return;
                }

                const map = App.state.mapboxMap;
                const sourceId = 'talhoes-source';
                const layerId = 'talhoes-layer';
                const borderLayerId = 'talhoes-border-layer';
                const labelLayerId = 'talhoes-labels';

                if (map.getSource(sourceId)) {
                    map.getSource(sourceId).setData(App.state.geoJsonData);
                } else {
                    map.addSource(sourceId, {
                        type: 'geojson',
                        data: App.state.geoJsonData,
                        generateId: true // Important for feature state
                    });
                }

                const themeColors = App.ui._getThemeColors();

                if (!map.getLayer(layerId)) {
                    map.addLayer({
                        id: layerId,
                        type: 'fill',
                        source: sourceId,
                        paint: {
                            'fill-color': [
                                'case',
                                ['boolean', ['feature-state', 'selected'], false], themeColors.primary,
                                ['boolean', ['feature-state', 'hover'], false], '#607D8B', // Lighter grey for hover
                                ['boolean', ['feature-state', 'risk'], false], '#d32f2f', // Red for risk
                                '#1C1C1C'
                            ],
                            'fill-opacity': [
                                'case',
                                ['boolean', ['feature-state', 'selected'], false], 0.9,
                                ['boolean', ['feature-state', 'hover'], false], 0.8,
                                ['boolean', ['feature-state', 'risk'], false], 0.6,
                                0.7 // Default opacity
                            ]
                        }
                    });
                }

                // Adicionar rótulos aos polígonos
                if (!map.getLayer(labelLayerId)) {
                    map.addLayer({
                        id: labelLayerId,
                        type: 'symbol',
                        source: sourceId,
                        minzoom: 10, // Show labels even earlier
                        layout: {
                            'symbol-placement': 'point',
                            'text-field': [
                                'format',
                                ['upcase', ['get', 'AGV_FUNDO']], { 'font-scale': 0.9 },
                                '\n', {},
                                ['upcase', ['get', 'AGV_TALHAO']], { 'font-scale': 1.2 }
                            ],
                            'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
                            'text-size': 14, // Larger font size
                            'text-ignore-placement': true,
                            'text-allow-overlap': true,
                            'text-pitch-alignment': 'viewport',
                        },
                        paint: {
                            'text-color': '#FFFFFF',
                            'text-halo-color': 'rgba(0, 0, 0, 0.9)', // Darker halo
                            'text-halo-width': 2 // Thicker halo for better contrast
                        }
                    });
                }

                if (!map.getLayer(borderLayerId)) {
                     map.addLayer({
                        id: borderLayerId,
                        type: 'line',
                        source: sourceId,
                        paint: {
                            'line-color': [
                                'case',
                                ['boolean', ['feature-state', 'selected'], false], '#00FFFF', // Ciano brilhante para selecionado
                                ['boolean', ['feature-state', 'searched'], false], '#00FFFF', // Ciano brilhante para pesquisado
                                '#FFFFFF' // Borda branca padrão
                            ],
                            'line-width': [
                                'case',
                                ['boolean', ['feature-state', 'selected'], false], 3,
                                ['boolean', ['feature-state', 'searched'], false], 4,
                                1.5 // Borda padrão mais sutil
                            ],
                            'line-opacity': 0.9
                        }
                    });
                }

                let hoveredFeatureId = null;

                map.on('mousemove', layerId, (e) => {
                    map.getCanvas().style.cursor = 'pointer';
                    if (e.features.length > 0) {
                        if (hoveredFeatureId !== null) {
                            map.setFeatureState({ source: sourceId, id: hoveredFeatureId }, { hover: false });
                        }
                        hoveredFeatureId = e.features[0].id;
                        map.setFeatureState({ source: sourceId, id: hoveredFeatureId }, { hover: true });
                    }
                });

                map.on('mouseleave', layerId, () => {
                    map.getCanvas().style.cursor = '';
                    if (hoveredFeatureId !== null) {
                        map.setFeatureState({ source: sourceId, id: hoveredFeatureId }, { hover: false });
                    }
                    hoveredFeatureId = null;
                });

                App.elements.monitoramentoAereo.btnToggleRiskView.style.display = 'flex';
                map.on('click', layerId, (e) => {
                    // Impede que o clique no talhão seja acionado se um marcador (armadilha) for clicado
                    if (e.originalEvent.target.closest('.mapboxgl-marker')) {
                        return;
                    }

                    if (e.features.length === 0) return;
                    const clickedFeature = e.features[0];
                    const userMarker = App.state.mapboxUserMarker;

                    if (App.state.trapPlacementMode === 'manual_select') {
                        // Não instala diretamente. Mostra um modal de confirmação primeiro.
                        const clickPosition = e.lngLat;
                        this.showTrapPlacementModal('manual_confirm', { feature: clickedFeature, position: clickPosition });
                    } else {
                        if (App.state.selectedMapFeature) {
                             map.setFeatureState({ source: sourceId, id: App.state.selectedMapFeature.id }, { selected: false });
                        }
                        
                        if (App.state.selectedMapFeature && App.state.selectedMapFeature.id === clickedFeature.id) {
                            App.state.selectedMapFeature = null;
                            this.hideTalhaoInfo();
                        } else {
                            App.state.selectedMapFeature = clickedFeature;
                            map.setFeatureState({ source: sourceId, id: clickedFeature.id }, { selected: true });

                            let riskPercentage = null;
                            if (App.state.riskViewActive) {
                                const farmCode = this._findProp(clickedFeature, ['FUNDO_AGR']);
                                if (App.state.farmRiskPercentages && App.state.farmRiskPercentages[farmCode] !== undefined) {
                                    riskPercentage = App.state.farmRiskPercentages[farmCode];
                                }
                            }
                            this.showTalhaoInfo(clickedFeature, riskPercentage);
                        }
                    }
                });
            },

            _findProp(feature, keys) {
                if (!feature || !feature.properties) return 'Não identificado';
                const props = {};
                // Normalize all property keys to uppercase for consistent access
                for (const key in feature.properties) {
                    props[key.toUpperCase()] = feature.properties[key];
                }
                
                for (const key of keys) {
                    if (props[key.toUpperCase()] !== undefined) {
                        return props[key.toUpperCase()];
                    }
                }
                return 'Não identificado';
            },

            // ALTERAÇÃO PONTO 5: Melhoria na busca de propriedades do Shapefile
            showTalhaoInfo(feature, riskPercentage = null) { // feature is now a GeoJSON feature
                const fundoAgricola = this._findProp(feature, ['FUNDO_AGR']);
                const fazendaNome = this._findProp(feature, ['NM_IMOVEL', 'NM_FAZENDA', 'NOME_FAZEN', 'FAZENDA']);
                const talhaoNome = this._findProp(feature, ['CD_TALHAO', 'COD_TALHAO', 'TALHAO']);
                const areaHa = this._findProp(feature, ['AREA_HA', 'AREA', 'HECTARES']);
                const variedade = this._findProp(feature, ['VARIEDADE', 'CULTURA']);

                const riskInfoHTML = riskPercentage !== null ? `
                    <div class="info-item risk-info">
                        <span class="label"><i class="fas fa-exclamation-triangle"></i> Risco de Aplicação</span>
                        <span class="value">${riskPercentage.toFixed(2)}%</span>
                    </div>
                ` : '';

                const contentEl = App.elements.monitoramentoAereo.infoBoxContent;
                contentEl.innerHTML = `
                    <div class="info-title">
                        <i class="fas fa-map-marker-alt"></i>
                        <span>Informações do Talhão</span>
                    </div>
                    ${riskInfoHTML}
                    <div class="info-item">
                        <span class="label">Fundo Agrícola</span>
                        <span class="value">${fundoAgricola}</span>
                    </div>
                    <div class="info-item">
                        <span class="label">Fazenda</span>
                        <span class="value">${fazendaNome}</span>
                    </div>
                    <div class="info-item">
                        <span class="label">Talhão</span>
                        <span class="value">${talhaoNome}</span>
                    </div>
                    <div class="info-item">
                        <span class="label">Variedade</span>
                        <span class="value">${variedade}</span>
                    </div>
                    <div class="info-item">
                        <span class="label">Área Total</span>
                        <span class="value">${(typeof areaHa === 'number' ? areaHa : 0).toFixed(2).replace('.',',')} ha</span>
                    </div>
                    <div class="info-box-actions" style="padding: 10px 20px 20px 20px;">
                        <button class="btn-download-map save" style="width: 100%;">
                            <i class="fas fa-cloud-download-alt"></i> Baixar Mapa Offline
                        </button>
                    </div>
                    <div class="download-progress-container" style="display: none; padding: 0 20px 20px 20px;">
                        <p class="download-progress-text" style="margin-bottom: 5px; font-size: 14px; color: var(--color-text-light);"></p>
                        <progress class="download-progress-bar" value="0" max="100" style="width: 100%;"></progress>
                    </div>
                `;

                contentEl.querySelector('.btn-download-map').addEventListener('click', () => {
                    this.startOfflineMapDownload(feature);
                });
                
                this.hideTrapInfo();
                App.elements.monitoramentoAereo.infoBox.classList.add('visible');
            },

            tileMath: {
                long2tile(lon, zoom) { return (Math.floor((lon + 180) / 360 * Math.pow(2, zoom))); },
                lat2tile(lat, zoom) { return (Math.floor((1 - Math.log(Math.tan(lat * Math.PI / 180) + 1 / Math.cos(lat * Math.PI / 180)) / Math.PI) / 2 * Math.pow(2, zoom))); },
                tile2long(x, z) { return (x / Math.pow(2, z) * 360 - 180); },
                tile2lat(y, z) {
                    const n = Math.PI - 2 * Math.PI * y / Math.pow(2, z);
                    return (180 / Math.PI * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n))));
                }
            },

            startOfflineMapDownload(feature) {
                const ZOOM_LEVELS = [14, 15, 16, 17];
                const infoBox = App.elements.monitoramentoAereo.infoBox;
                const progressContainer = infoBox.querySelector('.download-progress-container');
                const progressText = infoBox.querySelector('.download-progress-text');
                const progressBar = infoBox.querySelector('.download-progress-bar');

                const bbox = turf.bbox(feature);
                const [minLng, minLat, maxLng, maxLat] = bbox;

                let totalTilesToDownload = 0;
                const allTileUrls = [];

                ZOOM_LEVELS.forEach(zoom => {
                    const minX = this.tileMath.long2tile(minLng, zoom);
                    const maxX = this.tileMath.long2tile(maxLng, zoom);
                    const minY = this.tileMath.lat2tile(maxLat, zoom);
                    const maxY = this.tileMath.lat2tile(minLat, zoom);

                    for (let x = minX; x <= maxX; x++) {
                        for (let y = minY; y <= maxY; y++) {
                            const satelliteUrl = `https://api.mapbox.com/v4/mapbox.satellite/${zoom}/${x}/${y}@2x.png?access_token=${mapboxgl.accessToken}`;
                            const streetsUrl = `https://api.mapbox.com/v4/mapbox.mapbox-streets-v8/${zoom}/${x}/${y}@2x.png?access_token=${mapboxgl.accessToken}`;
                            allTileUrls.push(satelliteUrl);
                            allTileUrls.push(streetsUrl);
                            totalTilesToDownload += 2;
                        }
                    }
                });

                infoBox.querySelector('.btn-download-map').style.display = 'none';
                progressContainer.style.display = 'block';
                progressText.textContent = `A preparar para baixar ${totalTilesToDownload} tiles...`;
                progressBar.value = 0;
                progressBar.max = totalTilesToDownload;

                this.downloadTiles(allTileUrls);
            },

            async downloadTiles(urls) {
                const infoBox = App.elements.monitoramentoAereo.infoBox;
                const progressContainer = infoBox.querySelector('.download-progress-container');
                const progressText = infoBox.querySelector('.download-progress-text');
                const progressBar = infoBox.querySelector('.download-progress-bar');
                let downloadedCount = 0;
                let failedCount = 0;

                const db = await OfflineDB.dbPromise;

                for (const url of urls) {
                    try {
                        const existing = await db.get('offline-map-tiles', url);
                        if (existing) {
                            downloadedCount++;
                        } else {
                            const response = await fetch(url);
                            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
                            const blob = await response.blob();
                            await db.put('offline-map-tiles', blob, url);
                            downloadedCount++;
                        }
                    } catch (error) {
                        failedCount++;
                        console.warn(`Falha ao baixar ou guardar o tile: ${url}`, error);
                    }

                    progressBar.value = downloadedCount + failedCount;
                    progressText.textContent = `A baixar... ${downloadedCount}/${urls.length} (Falhas: ${failedCount})`;
                }

                if (failedCount > 0) {
                     App.ui.showAlert(`Download concluído com ${failedCount} falhas.`, 'warning');
                } else {
                     App.ui.showAlert('Download do mapa offline concluído com sucesso!', 'success');
                }

                setTimeout(() => {
                    progressContainer.style.display = 'none';
                    infoBox.querySelector('.btn-download-map').style.display = 'block';
                }, 5000);
            },

            hideTalhaoInfo() {
                if (App.state.selectedMapFeature) {
                    App.state.mapboxMap.setFeatureState({ source: 'talhoes-source', id: App.state.selectedMapFeature.id }, { selected: false });
                    App.state.selectedMapFeature = null;
                }
                App.elements.monitoramentoAereo.infoBox.classList.remove('visible');
            },

            loadTraps() {
                Object.values(App.state.mapboxTrapMarkers).forEach(marker => marker.remove());
                App.state.mapboxTrapMarkers = {};

                App.state.armadilhas.forEach(trap => {
                    if (trap.status === 'Ativa') {
                        this.addOrUpdateTrapMarker(trap);
                    }
                });
            },

            addOrUpdateTrapMarker(trap) {
                if (!trap.dataInstalacao || !App.state.mapboxMap) return;

                // Lida com ambos os Timestamps do Firebase (que têm .toDate()) e Date objects/ISO strings (que não têm)
                const installDate = typeof trap.dataInstalacao.toDate === 'function'
                    ? trap.dataInstalacao.toDate()
                    : new Date(trap.dataInstalacao);

                const now = new Date();
                const diasDesdeInstalacao = Math.floor((now - installDate) / (1000 * 60 * 60 * 24));

                let color = '#388e3c'; // Verde (Normal)
                if (diasDesdeInstalacao >= 5 && diasDesdeInstalacao <= 7) {
                    color = '#f57c00'; // Amarelo (Atenção)
                } else if (diasDesdeInstalacao > 7) {
                    color = '#d32f2f'; // Vermelho (Atrasado)
                }
                
                const el = document.createElement('div');
                el.className = 'mapbox-marker';
                el.style.width = '30px';
                el.style.height = '30px';
                el.style.borderRadius = '50%';
                el.style.backgroundColor = color;
                el.style.border = '2px solid white';
                el.style.display = 'flex';
                el.style.justifyContent = 'center';
                el.style.alignItems = 'center';
                el.style.cursor = 'pointer';
                el.innerHTML = '<i class="fas fa-bug" style="color: white; font-size: 16px;"></i>';
                el.title = `Armadilha instalada em ${installDate.toLocaleDateString()}`;

                if (App.state.mapboxTrapMarkers[trap.id]) {
                    App.state.mapboxTrapMarkers[trap.id].getElement().style.backgroundColor = color;
                } else {
                    const marker = new mapboxgl.Marker(el)
                        .setLngLat([trap.longitude, trap.latitude])
                        .addTo(App.state.mapboxMap);
                    
                    el.addEventListener('click', (e) => { e.stopPropagation(); this.showTrapInfo(trap.id); });
                    App.state.mapboxTrapMarkers[trap.id] = marker;
                }
            },

            promptInstallTrap() {
                if (!App.state.mapboxUserMarker) {
                    App.ui.showAlert("Localização do usuário não disponível para instalar a armadilha.", "error");
                    return;
                }
                this.showTrapPlacementModal('loading');
                const position = App.state.mapboxUserMarker.getLngLat();
                this.findTalhaoFromLocation(position);
            },

            findTalhaoFromLocation(position) { // position is a Mapbox LngLat object
                const containingTalhoes = [];
                const point = turf.point([position.lng, position.lat]);
                // Cria um buffer de 15 metros ao redor do ponto do usuário para compensar a imprecisão do GPS.
                const buffer = turf.buffer(point, 15, { units: 'meters' });
                const allTalhoes = App.state.geoJsonData;

                if (!allTalhoes || !allTalhoes.features) {
                    this.showTrapPlacementModal('failure');
                    return;
                }

                allTalhoes.features.forEach(feature => {
                    try {
                        // Verifica se o buffer do usuário (e não o ponto exato) cruza com o polígono do talhão.
                        if (!turf.booleanDisjoint(buffer, feature.geometry)) {
                            containingTalhoes.push(feature);
                        }
                    } catch (e) {
                        // Adicionado para graciosamente ignorar geometrias inválidas que podem vir do shapefile
                        console.warn("Geometria inválida ou erro no processamento do Turf.js:", e, feature.geometry);
                    }
                });

                if (containingTalhoes.length === 1) {
                    this.showTrapPlacementModal('success', containingTalhoes);
                } else if (containingTalhoes.length > 1) {
                    this.showTrapPlacementModal('conflict', containingTalhoes);
                } else {
                    this.showTrapPlacementModal('failure');
                }
            },

            showTrapPlacementModal(state, data = null) {
                const { overlay, body, confirmBtn, manualBtn } = App.elements.trapPlacementModal;
                let content = '';
                
                confirmBtn.style.display = 'none';
                manualBtn.style.display = 'inline-flex';

                switch(state) {
                    case 'loading':
                        content = `<div class="spinner"></div><p style="margin-left: 15px;">A detetar talhão...</p>`;
                        manualBtn.style.display = 'none';
                        break;
                    case 'success':
                        const feature = data[0];
                        const fazendaNome = this._findProp(feature, ['NM_IMOVEL', 'NM_FAZENDA', 'NOME_FAZEN', 'FAZENDA']);
                        const talhaoName = this._findProp(feature, ['CD_TALHAO', 'COD_TALHAO', 'TALHAO']);
                        const fundoAgricola = this._findProp(feature, ['FUNDO_AGR']);

                        content = `<p style="font-weight: 500;">Confirme o local de instalação:</p>
                                   <div class="location-confirmation-box">
                                       <span><strong>Fundo Agrícola:</strong> ${fundoAgricola}</span>
                                       <span><strong>Fazenda:</strong> ${fazendaNome}</span>
                                       <span><strong>Talhão:</strong> ${talhaoName}</span>
                                   </div>
                                   <p>Deseja instalar a armadilha neste local?</p>`;
                        confirmBtn.style.display = 'inline-flex';
                        App.state.trapPlacementData = { feature: feature };
                        break;
                    case 'conflict':
                        content = `<p>Vários talhões detetados na sua localização. Por favor, selecione o correto:</p><div id="talhao-conflict-list" style="margin-top:15px; text-align:left;">`;
                        data.forEach((f, index) => {
                            const name = f.properties.CD_TALHAO || f.properties.TALHAO || `Opção ${index + 1}`;
                            content += `<label class="report-option-item" style="margin-bottom:10px;"><input type="radio" name="talhaoConflict" value="${index}"><span class="checkbox-visual"><i class="fas fa-check"></i></span><span class="option-content">${name}</span></label>`;
                        });
                        content += `</div>`;
                        confirmBtn.style.display = 'inline-flex';
                        App.state.trapPlacementData = { features: data };
                        break;
                    case 'failure':
                        content = `<p style="text-align: center;"><i class="fas fa-exclamation-triangle fa-2x" style="color: var(--color-warning); margin-bottom: 10px;"></i><br>Você precisa estar <strong>dentro de um talhão</strong> para a instalação automática.<br><br>Se necessário, use a opção de seleção manual.</p>`;
                        break;
                    case 'manual_confirm':
                        const manualFeature = data.feature;
                        const manualFazendaNome = this._findProp(manualFeature, ['NM_IMOVEL', 'NM_FAZENDA', 'NOME_FAZEN', 'FAZENDA']);
                        const manualTalhaoName = this._findProp(manualFeature, ['CD_TALHAO', 'COD_TALHAO', 'TALHAO']);
                        const manualFundoAgricola = this._findProp(manualFeature, ['FUNDO_AGR']);

                        content = `<p style="font-weight: 500;">Confirmar instalação manual:</p>
                                   <div class="location-confirmation-box">
                                       <span><strong>Fundo Agrícola:</strong> ${manualFundoAgricola}</span>
                                       <span><strong>Fazenda:</strong> ${manualFazendaNome}</span>
                                       <span><strong>Talhão:</strong> ${manualTalhaoName}</span>
                                   </div>
                                   <p>Deseja instalar a armadilha neste talhão selecionado?</p>`;
                        confirmBtn.style.display = 'inline-flex';
                        manualBtn.style.display = 'none';
                        App.state.trapPlacementData = { feature: manualFeature, position: data.position };
                        break;
                    case 'manual_select':
                        content = `<p style="font-weight: 500; text-align: center;">Clique no talhão desejado no mapa para o selecionar.</p>`;
                        manualBtn.style.display = 'none';
                        break;
                }
                
                body.innerHTML = content;
                overlay.classList.add('show');
                App.state.trapPlacementMode = state;
            },

            hideTrapPlacementModal() {
                 App.elements.trapPlacementModal.overlay.classList.remove('show');
                 App.state.trapPlacementMode = null;
                 App.state.trapPlacementData = null;
            },

            async installTrap(lat, lng, feature = null) {
                const installDate = new Date();
                const trapId = `trap_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

                const newTrapData = {
                    id: trapId,
                    latitude: lat,
                    longitude: lng,
                    dataInstalacao: installDate.toISOString(),
                    instaladoPor: App.state.currentUser.uid,
                    status: "Ativa",
                    fazendaNome: feature ? this._findProp(feature, ['NM_IMOVEL', 'NM_FAZENDA', 'NOME_FAZEN', 'FAZENDA']) : 'Não identificado',
                    fazendaCode: feature ? this._findProp(feature, ['FUNDO_AGR']) : null,
                    talhaoNome: feature ? this._findProp(feature, ['CD_TALHAO', 'COD_TALHAO', 'TALHAO']) : 'Não identificado',
                    companyId: App.state.currentUser.companyId
                };

                App.ui.setLoading(true, "A guardar armadilha...");

                try {
                    if (navigator.onLine) {
                        const dataForFirestore = { ...newTrapData, dataInstalacao: Timestamp.fromDate(installDate) };
                        await App.data.setDocument('armadilhas', trapId, dataForFirestore);
                        this.addOrUpdateTrapMarker({ id: trapId, ...dataForFirestore });
                        App.ui.showAlert(`Armadilha ${trapId.substring(0, 9)}... instalada com sucesso.`, "success");
                    } else {
                        await OfflineDB.add('offline-writes', { id: trapId, collection: 'armadilhas', data: newTrapData });
                        App.ui.showAlert('Armadilha guardada offline. Será enviada quando houver conexão.', 'info');
                        const tempTrapForMarker = { ...newTrapData, dataInstalacao: installDate };
                        this.addOrUpdateTrapMarker(tempTrapForMarker);
                    }
                } catch (error) {
                    console.error("Erro ao instalar armadilha, tentando guardar offline:", error);
                    try {
                        await OfflineDB.add('offline-writes', { id: trapId, collection: 'armadilhas', data: newTrapData });
                        App.ui.showAlert('Falha ao conectar. Armadilha guardada offline.', 'warning');
                        const tempTrapForMarker = { ...newTrapData, dataInstalacao: installDate };
                        this.addOrUpdateTrapMarker(tempTrapForMarker);
                    } catch (offlineError) {
                        console.error("Falha crítica ao guardar armadilha offline:", offlineError);
                        App.ui.showAlert("Falha crítica ao guardar a armadilha offline.", "error");
                    }
                } finally {
                    App.ui.setLoading(false);
                }
            },

            promptCollectTrap(trapId) {
                const trap = App.state.armadilhas.find(t => t.id === trapId);
                if (!trap) return;

                App.ui.showConfirmationModal(
                    `Confirmar coleta para a armadilha em ${trap.talhaoNome || 'local desconhecido'}?`,
                    async (inputs) => {
                        const mothCount = parseInt(inputs.count, 10);
                        if (isNaN(mothCount) || mothCount < 0) {
                            App.ui.showAlert("Por favor, insira um número válido de mariposas.", "error");
                            return;
                        }
                        await this.collectTrap(trapId, mothCount, inputs.observations);
                    },
                    [
                        { id: 'count', placeholder: 'Nº de mariposas capturadas', type: 'number', required: true },
                        { id: 'observations', placeholder: 'Adicionar observações (opcional)', type: 'textarea', value: trap.observacoes || '' }
                    ]
                );
            },

            async collectTrap(trapId, count, observations) {
                const collectionTime = new Date();
                const updateData = {
                    status: "Coletada",
                    dataColeta: collectionTime.toISOString(), // Use ISO String for offline storage
                    coletadoPor: App.state.currentUser.uid,
                    contagemMariposas: count,
                    observacoes: observations || null
                };

                // Optimistic UI Update
                if (App.state.mapboxTrapMarkers[trapId]) {
                    App.state.mapboxTrapMarkers[trapId].remove();
                    delete App.state.mapboxTrapMarkers[trapId];
                }
                this.hideTrapInfo();
                App.ui.showAlert("Coleta registrada. Sincronizando...", "info");

                const trapIndex = App.state.armadilhas.findIndex(t => t.id === trapId);
                if (trapIndex > -1) {
                    App.state.armadilhas[trapIndex].status = "Coletada";
                }
                this.checkTrapStatusAndNotify();

                try {
                    if (!navigator.onLine) {
                        throw new Error("Offline mode detected");
                    }
                    // For online, use Firestore Timestamp
                    const onlineUpdateData = { ...updateData, dataColeta: Timestamp.fromDate(collectionTime) };
                    await App.data.updateDocument('armadilhas', trapId, onlineUpdateData);
                    App.ui.showAlert("Coleta sincronizada com sucesso!", "success");

                } catch (error) {
                    console.error("Erro ao registrar coleta online, salvando offline:", error);
                    try {
                        await OfflineDB.add('offline-writes', {
                            id: `collect_${trapId}_${Date.now()}`, // Unique ID for the write operation
                            type: 'update', // Specify the operation type
                            collection: 'armadilhas',
                            docId: trapId, // The document to update
                            data: updateData // The data for the update
                        });
                        App.ui.showAlert("Coleta salva offline. Será sincronizada quando houver conexão.", "info");
                    } catch (offlineError) {
                        console.error("Falha crítica ao salvar coleta offline:", offlineError);
                        App.ui.showAlert("Falha crítica ao salvar a coleta offline.", "error");
                        // Revert optimistic UI update if offline save also fails
                        const trap = App.state.armadilhas.find(t => t.id === trapId);
                        if (trap) {
                            trap.status = "Ativa";
                            this.addOrUpdateTrapMarker(trap);
                        }
                    }
                }
            },

            async deleteTrap(trapId) {
                App.ui.showConfirmationModal(
                    "Tem a certeza que deseja excluir esta armadilha? Esta ação é irreversível.",
                    async () => {
                        try {
                            await App.data.deleteDocument('armadilhas', trapId);
                            
                            if (App.state.mapboxTrapMarkers[trapId]) {
                                App.state.mapboxTrapMarkers[trapId].remove();
                                delete App.state.mapboxTrapMarkers[trapId];
                            }
                            
                            App.state.armadilhas = App.state.armadilhas.filter(t => t.id !== trapId);

                            App.ui.showAlert("Armadilha excluída com sucesso.", "info");
                            this.hideTrapInfo();
                        } catch (error) {
                            console.error("Erro ao excluir armadilha:", error);
                            App.ui.showAlert("Falha ao excluir armadilha.", "error");
                        }
                    }
                );
            },

            async editTrap(trapId) {
                const trap = App.state.armadilhas.find(t => t.id === trapId);
                if (!trap) return;

                App.ui.showConfirmationModal(
                    `Editar observações para a armadilha em ${trap.talhaoNome || 'local desconhecido'}:`,
                    async (newObservations) => {
                        if (newObservations === null) return;
                        try {
                            await App.data.updateDocument('armadilhas', trapId, { observacoes: newObservations });
                            trap.observacoes = newObservations;
                            this.showTrapInfo(trapId);
                            App.ui.showAlert("Observações atualizadas.", "success");
                        } catch (error) {
                            console.error("Erro ao editar armadilha:", error);
                            App.ui.showAlert("Falha ao atualizar observações.", "error");
                        }
                    },
                    true // needsInput
                );
                
                const input = App.elements.confirmationModal.input;
                input.value = trap.observacoes || '';
                input.placeholder = 'Digite suas observações...';
                App.elements.confirmationModal.confirmBtn.textContent = "Salvar";
            },
            
            showTrapInfo(trapId) {
                try {
                    const trap = App.state.armadilhas.find(t => t.id === trapId);
                    if (!trap) return;

                    const installDate = typeof trap.dataInstalacao.toDate === 'function'
                        ? trap.dataInstalacao.toDate()
                        : new Date(trap.dataInstalacao);

                    if (isNaN(installDate.getTime())) {
                        throw new Error("A data de instalação da armadilha é inválida.");
                    }

                    const collectionDate = new Date(installDate);
                    collectionDate.setDate(installDate.getDate() + 7);
                    const now = new Date();

                    const diasDesdeInstalacao = Math.floor((now - installDate) / (1000 * 60 * 60 * 24));

                    let statusText = 'Normal';
                    let statusColor = 'var(--color-success)';
                    if (diasDesdeInstalacao >= 5 && diasDesdeInstalacao <= 7) {
                        const diasRestantes = 7 - diasDesdeInstalacao;
                        statusText = `Atenção (${diasRestantes} dias restantes)`;
                        statusColor = 'var(--color-warning)';
                    } else if (diasDesdeInstalacao > 7) {
                        const diasAtraso = diasDesdeInstalacao - 7;
                        statusText = `Atrasado (${diasAtraso} dias)`;
                        statusColor = 'var(--color-danger)';
                    }

                    const contentEl = App.elements.monitoramentoAereo.trapInfoBoxContent;
                    contentEl.innerHTML = `
                        <div class="info-title" style="color: ${statusColor};">
                            <i class="fas fa-bug"></i>
                            <span>Detalhes da Armadilha</span>
                        </div>
                        <div class="info-item">
                            <span class="label">Status</span>
                            <span class="value"><span class="status-indicator" style="background-color: ${statusColor};"></span>${statusText}</span>
                        </div>
                        <div class="info-item">
                            <span class="label">Fazenda</span>
                            <span class="value">${trap.fazendaNome || 'N/A'}</span>
                        </div>
                        <div class="info-item">
                            <span class="label">Talhão</span>
                            <span class="value">${trap.talhaoNome || 'N/A'}</span>
                        </div>
                        <div class="info-item">
                            <span class="label">Data de Instalação</span>
                            <span class="value">${installDate.toLocaleDateString('pt-BR')}</span>
                        </div>
                        <div class="info-item">
                            <span class="label">Data Prevista para Coleta</span>
                            <span class="value">${collectionDate.toLocaleDateString('pt-BR')}</span>
                        </div>
                        <div class="info-item" id="trap-obs-display" style="${trap.observacoes ? 'display: flex;' : 'display: none;'}">
                            <span class="label">Observações</span>
                            <span class="value" style="white-space: pre-wrap; font-size: 14px;">${trap.observacoes || ''}</span>
                        </div>
                        <div class="info-box-actions">
                            <button class="btn-collect-trap" id="btnCollectTrap"><i class="fas fa-check-circle"></i> Coletar</button>
                            <div class="action-button-group">
                                <button class="action-btn" id="btnEditTrap" title="Editar Observações"><i class="fas fa-edit"></i></button>
                                <button class="action-btn danger" id="btnDeleteTrap" title="Excluir Armadilha"><i class="fas fa-trash"></i></button>
                            </div>
                        </div>
                    `;

                    document.getElementById('btnCollectTrap').onclick = () => this.promptCollectTrap(trapId);
                    document.getElementById('btnEditTrap').onclick = () => this.editTrap(trapId);
                    document.getElementById('btnDeleteTrap').onclick = () => this.deleteTrap(trapId);

                    this.hideTalhaoInfo();
                    App.elements.monitoramentoAereo.trapInfoBox.classList.add('visible');
                } catch (error) {
                    console.error("Erro ao exibir informações da armadilha:", error);
                    App.ui.showAlert(`Não foi possível carregar os dados desta armadilha. Pode haver dados corrompidos. Erro: ${error.message}`, "error", 5000);
                }
            },

            hideTrapInfo() {
                App.elements.monitoramentoAereo.trapInfoBox.classList.remove('visible');
            },
            
            // Verifica o status das armadilhas para gerar notificações de coleta
            checkTrapStatusAndNotify() {
                const activeTraps = App.state.armadilhas.filter(t => t.status === 'Ativa');
                let newNotificationsForBell = [];
                
                activeTraps.forEach(trap => {
                    if (!trap.dataInstalacao) {
                        return;
                    }

                    // Lida com ambos os Timestamps do Firebase (que têm .toDate()) e Date objects/ISO strings (que não têm)
                    const installDate = typeof trap.dataInstalacao.toDate === 'function'
                        ? trap.dataInstalacao.toDate()
                        : new Date(trap.dataInstalacao);
                    const now = new Date();

                    if (isNaN(installDate.getTime())) {
                        console.error(`Armadilha ${trap.id} com data de instalação inválida.`);
                        return;
                    }

                    const diasDesdeInstalacao = Math.floor((now - installDate) / (1000 * 60 * 60 * 24));

                    let notification = null;
                    if (diasDesdeInstalacao >= 5 && diasDesdeInstalacao <= 7) {
                        const diasRestantes = 7 - diasDesdeInstalacao;
                        const msg = diasRestantes > 0 ? `Coleta em ${diasRestantes} dia(s).` : "Coleta hoje.";
                        notification = { trapId: trap.id, type: 'warning', message: msg, timestamp: new Date() };
                    } else if (diasDesdeInstalacao > 7) {
                        const diasAtraso = diasDesdeInstalacao - 7;
                        notification = { trapId: trap.id, type: 'danger', message: `Coleta atrasada em ${diasAtraso} dia(s).`, timestamp: new Date() };
                    }

                    if (notification) {
                        // Adiciona para a lista do sino
                        newNotificationsForBell.push(notification);

                        // Mostra o pop-up apenas se não foi mostrado nesta sessão
                        if (!App.state.notifiedTrapIds.has(trap.id)) {
                            this.showTrapNotification(notification);
                            App.state.notifiedTrapIds.add(trap.id);
                            sessionStorage.setItem('notifiedTrapIds', JSON.stringify(Array.from(App.state.notifiedTrapIds)));
                        }
                    }
                });

                // Atualiza o estado geral de notificações
                const unreadNotifications = newNotificationsForBell.filter(n => !App.state.trapNotifications.some(oldN => oldN.trapId === n.trapId && oldN.message === n.message));
                if (unreadNotifications.length > 0) {
                    App.state.unreadNotificationCount += unreadNotifications.length;
                }
                App.state.trapNotifications = newNotificationsForBell.sort((a, b) => b.timestamp - a.timestamp);
                App.ui.updateNotificationBell();
            },

            showTrapNotification(notification) {
                const container = App.elements.notificationContainer;

                // Limita o número de notificações na tela para 3
                while (container.children.length >= 3) {
                    container.removeChild(container.firstChild);
                }

                const notificationEl = document.createElement('div');
                notificationEl.className = `trap-notification ${notification.type}`;
                notificationEl.dataset.trapId = notification.trapId;

                const iconClass = notification.type === 'warning' ? 'fa-exclamation-triangle' : 'fa-exclamation-circle';
                
                notificationEl.innerHTML = `
                    <button class="close-btn">&times;</button>
                    <div class="icon"><i class="fas ${iconClass}"></i></div>
                    <div class="text">
                        <p><strong>Armadilha requer atenção</strong></p>
                        <p>${notification.message}</p>
                    </div>
                `;
                
                container.appendChild(notificationEl);
                
                const dismiss = () => {
                    notificationEl.classList.add('dismiss');
                    notificationEl.addEventListener('animationend', () => {
                        notificationEl.remove();
                    });
                };

                // Click no X para fechar
                notificationEl.querySelector('.close-btn').addEventListener('click', dismiss);

                // Deslizar para fechar
                let touchStartX = 0;
                let touchEndX = 0;

                notificationEl.addEventListener('touchstart', (event) => {
                    touchStartX = event.changedTouches[0].screenX;
                }, { passive: true });

                notificationEl.addEventListener('touchend', (event) => {
                    touchEndX = event.changedTouches[0].screenX;
                    if (touchEndX < touchStartX - 50) { // Deslize para a esquerda de 50px
                        dismiss();
                    }
                }, { passive: true });

                // Remover automaticamente após um tempo
                setTimeout(dismiss, 10000);
            },

            toggleRiskView() {
                App.state.riskViewActive = !App.state.riskViewActive;
                this.calculateAndApplyRiskView();
            },

            calculateAndApplyRiskView() {
                const map = App.state.mapboxMap;
                if (!map || !App.state.geoJsonData) return;

                console.log("--- [START] calculateAndApplyRiskView ---");

                // Limpa o estado de risco de features anteriormente destacadas
                if (map.riskFarmFeatureIds) {
                    map.riskFarmFeatureIds.forEach(id => {
                        map.setFeatureState({ source: 'talhoes-source', id: id }, { risk: false });
                    });
                }
                map.riskFarmFeatureIds = [];

                if (!App.state.riskViewActive) {
                    App.elements.monitoramentoAereo.btnToggleRiskView.classList.remove('active');

                    const themeColors = App.ui._getThemeColors();
                    // Restaura as propriedades de pintura originais para a visualização normal
                    map.setPaintProperty('talhoes-layer', 'fill-color', [
                        'case',
                        ['boolean', ['feature-state', 'selected'], false], themeColors.primary,
                        ['boolean', ['feature-state', 'hover'], false], '#607D8B', // Cinza claro para hover
                        ['boolean', ['feature-state', 'risk'], false], '#d32f2f', // Vermelho para risco
                        '#1C1C1C' // Cinza escuro padrão
                    ]);
                    map.setPaintProperty('talhoes-layer', 'fill-opacity', [
                        'case',
                        ['boolean', ['feature-state', 'selected'], false], 0.8,
                        ['boolean', ['feature-state', 'hover'], false], 0.7,
                        ['boolean', ['feature-state', 'risk'], false], 0.6,
                        0.5 // Opacidade padrão
                    ]);
                    map.setPaintProperty('talhoes-border-layer', 'line-opacity', 0.9);

                    // Garante que todos os rótulos sejam exibidos ao desativar a visualização de risco
                    map.setFilter('talhoes-labels', null);
                    this.loadTraps();
                    console.log("Risk view desativada. Revertendo para a visualização padrão.");
                    console.log("--- [END] calculateAndApplyRiskView ---");
                    return;
                }

                // Se a visualização de risco está ativa, preparamos o UI
                App.elements.monitoramentoAereo.btnToggleRiskView.classList.add('active');
                Object.values(App.state.mapboxTrapMarkers).forEach(marker => marker.remove());
                App.state.mapboxTrapMarkers = {};

                // --- 1. CALCULAR O RISCO PRIMEIRO ---
                const currentUserCompanyId = App.state.currentUser.companyId;
                if (!currentUserCompanyId && App.state.currentUser.role !== 'super-admin') {
                    App.ui.showAlert("A sua conta não está associada a uma empresa.", "error");
                    console.log("--- [END] calculateAndApplyRiskView ---");
                    return;
                }

                const farmsInRisk = new Set();
                const farmRiskPercentages = {};

                const allFarms = App.state.fazendas.filter(f => f.companyId === currentUserCompanyId);
                const companyTraps = App.state.armadilhas.filter(t => t.companyId === currentUserCompanyId);
                const collectedTraps = companyTraps.filter(t => t.status === 'Coletada');

                console.log(`[RISK_DEBUG] Encontradas ${allFarms.length} fazendas, ${companyTraps.length} armadilhas no total, ${collectedTraps.length} armadilhas coletadas para a empresa.`);

                allFarms.forEach(farm => {
                    const collectedTrapsOnFarm = collectedTraps.filter(t =>
                        (t.fazendaCode ? parseInt(String(t.fazendaCode).trim()) === parseInt(String(farm.code).trim()) : t.fazendaNome === farm.name)
                    );

                    if (collectedTrapsOnFarm.length === 0) {
                        farmRiskPercentages[farm.code] = 0;
                        return; // Skip if no collections, risk is 0
                    }

                    // 1. Find the most recent collection date on this farm
                    let mostRecentCollectionDate = new Date(0);
                    collectedTrapsOnFarm.forEach(trap => {
                        const collectionDate = trap.dataColeta?.toDate ? trap.dataColeta.toDate() : new Date(trap.dataColeta);
                        if (collectionDate > mostRecentCollectionDate) {
                            mostRecentCollectionDate = collectionDate;
                        }
                    });

                    // 2. Filter to get only collections from that specific day (the monitoring cycle)
                    const latestCycleCollections = collectedTrapsOnFarm.filter(trap => {
                        const collectionDate = trap.dataColeta?.toDate ? trap.dataColeta.toDate() : new Date(trap.dataColeta);
                        return collectionDate.getFullYear() === mostRecentCollectionDate.getFullYear() &&
                               collectionDate.getMonth() === mostRecentCollectionDate.getMonth() &&
                               collectionDate.getDate() === mostRecentCollectionDate.getDate();
                    });

                    // 3. Deduplicate collections for the same trap, keeping only the latest one by time
                    const latestUniqueCollections = new Map();
                    latestCycleCollections.forEach(trap => {
                        // A trap is uniquely identified by its ID
                        const trapKey = trap.id;
                        const existing = latestUniqueCollections.get(trapKey);
                        const collectionDate = trap.dataColeta?.toDate ? trap.dataColeta.toDate() : new Date(trap.dataColeta);
                        if (!existing || collectionDate > (existing.dataColeta?.toDate ? existing.dataColeta.toDate() : new Date(existing.dataColeta))) {
                            latestUniqueCollections.set(trapKey, trap);
                        }
                    });

                    const finalCycleTraps = Array.from(latestUniqueCollections.values());

                    // 4. Count high-risk traps within this final, unique set
                    const highCountTraps = finalCycleTraps.filter(t => t.contagemMariposas >= 6);

                    // Divisor is the number of traps collected in the latest cycle.
                    const divisor = finalCycleTraps.length;
                    const riskPercentage = divisor > 0 ? (highCountTraps.length / divisor) * 100 : 0;

                    farmRiskPercentages[farm.code] = riskPercentage;
                    if (riskPercentage > 30) {
                        farmsInRisk.add(parseInt(String(farm.code).trim(), 10));
                    }
                });

                App.state.farmRiskPercentages = farmRiskPercentages;
                console.log("[RISK_DEBUG] Códigos de fazendas em risco calculados:", Array.from(farmsInRisk));

                // --- 2. APLICAR ESTILOS COM BASE NOS RESULTADOS ---
                if (farmsInRisk.size > 0) {
                    console.log("[RISK_DEBUG] Fazendas em risco encontradas. Aplicando estilo de isolamento.");
                    // Isola as fazendas em risco, permitindo interação com elas
                    map.setPaintProperty('talhoes-layer', 'fill-color', [
                        'case',
                        ['boolean', ['feature-state', 'risk'], false], '#d32f2f', // Vermelho para risco
                        App.ui._getThemeColors().primary // Cor padrão (será invisível)
                    ]);
                    map.setPaintProperty('talhoes-layer', 'fill-opacity', [
                        'case',
                        ['boolean', ['feature-state', 'risk'], false],
                        ['case', ['boolean', ['feature-state', 'selected'], false], 0.85, ['boolean', ['feature-state', 'hover'], false], 0.6, 0.5],
                        0.0 // Invisível se não estiver em risco
                    ]);
                    map.setPaintProperty('talhoes-border-layer', 'line-opacity', [
                        'case',
                        ['boolean', ['feature-state', 'risk'], false], 0.9,
                        0.0 // Invisível se não estiver em risco
                    ]);

                    const allSourceFeatures = map.querySourceFeatures('talhoes-source');
                    const featuresToHighlight = allSourceFeatures.filter(feature => {
                        const farmCode = this._findProp(feature, ['FUNDO_AGR']);
                        return farmsInRisk.has(parseInt(String(farmCode).trim(), 10));
                    });

                    if (featuresToHighlight.length > 0) {
                        const featureIds = featuresToHighlight.map(f => f.id);
                        featureIds.forEach(id => {
                            map.setFeatureState({ source: 'talhoes-source', id: id }, { risk: true });
                        });
                        map.riskFarmFeatureIds = featureIds;

                        // Get the string representations of the farm codes in risk
                        const farmCodesInRiskAsStrings = Array.from(farmsInRisk, code => String(code));

                        // Filter labels to show only those for farms in risk
                        const labelFilter = ['in', ['get', 'AGV_FUNDO'], ['literal', farmCodesInRiskAsStrings]];
                        map.setFilter('talhoes-labels', labelFilter);

                        App.ui.showAlert(`${farmsInRisk.size} fazenda(s) em risco foram destacadas.`, 'info');
                    } else {
                         // This can happen if the farm code in risk doesn't match any map feature
                        console.warn("[RISK_DEBUG] Risk farms calculated, but no corresponding features found on the map.");
                        App.ui.showAlert('Nenhuma fazenda em risco foi identificada no mapa.', 'success');
                         // Revert to default view to avoid a blank map
                        map.setPaintProperty('talhoes-layer', 'fill-color', App.ui._getThemeColors().primary);
                        map.setPaintProperty('talhoes-layer', 'fill-opacity', 0.5);
                        map.setPaintProperty('talhoes-border-layer', 'line-opacity', 0.9);
                        map.setFilter('talhoes-labels', null); // Show all labels
                    }

                } else {
                    console.log("[RISK_DEBUG] No risk farms found. Displaying all plots normally.");
                    App.ui.showAlert('Nenhuma fazenda em risco foi identificada no período.', 'success');
                    // Ensure the map doesn't stay blank by reverting to the default view
                    map.setPaintProperty('talhoes-layer', 'fill-color', '#1C1C1C');
                    map.setPaintProperty('talhoes-layer', 'fill-opacity', 0.7);
                    map.setPaintProperty('talhoes-border-layer', 'line-opacity', 0.9);
                    map.setFilter('talhoes-labels', null); // Show all labels
                }

                console.log("--- [END] calculateAndApplyRiskView ---");
            },

            centerOnTrap(trapId) {
                const marker = App.state.mapboxTrapMarkers[trapId];
                if (marker) {
                    const position = marker.getLngLat();
                    App.state.mapboxMap.flyTo({ center: position, zoom: 18 });
                    this.showTrapInfo(trapId);
                }
            },

            toggleSearch() {
                const searchContainer = document.querySelector('.map-search-container');
                const searchInput = App.elements.monitoramentoAereo.mapFarmSearchInput;
                const searchBtn = App.elements.monitoramentoAereo.mapFarmSearchBtn;
                const searchBtnIcon = searchBtn.querySelector('i');

                const isActive = searchContainer.classList.contains('active');

                if (isActive) {
                    // Se estiver ativo, verifica se tem texto para pesquisar, senão apenas fecha
                    if (searchInput.value.trim() !== '') {
                        this.searchFarmOnMap();
                    } else {
                        searchContainer.classList.remove('active');
                        searchBtnIcon.className = 'fas fa-search';
                        searchInput.value = '';
                    }
                } else {
                    // Se não estiver ativo, ativa
                    searchContainer.classList.add('active');
                    searchBtnIcon.className = 'fas fa-times'; // Ícone de fechar
                    searchInput.focus();
                }
            },

            closeSearch() {
                const searchContainer = document.querySelector('.map-search-container');
                const searchInput = App.elements.monitoramentoAereo.mapFarmSearchInput;
                const searchBtn = App.elements.monitoramentoAereo.mapFarmSearchBtn;
                const searchBtnIcon = searchBtn.querySelector('i');

                if (searchContainer.classList.contains('active')) {
                    searchContainer.classList.remove('active');
                    searchBtnIcon.className = 'fas fa-search';
                    searchInput.value = '';
                }
            },

            searchFarmOnMap() {
                const searchInput = App.elements.monitoramentoAereo.mapFarmSearchInput;
                const searchTerm = searchInput.value.trim().toUpperCase();
                if (!searchTerm) {
                    this.closeSearch();
                    return;
                }

                const { geoJsonData, mapboxMap } = App.state;
                if (!geoJsonData || !mapboxMap) {
                    App.ui.showAlert("Os dados do mapa ainda não foram carregados.", "error");
                    return;
                }

                // Limpa a pesquisa anterior
                if (mapboxMap.searchedFarmFeatureIds) {
                    mapboxMap.searchedFarmFeatureIds.forEach(id => {
                        mapboxMap.setFeatureState({ source: 'talhoes-source', id: id }, { searched: false });
                    });
                }
                mapboxMap.searchedFarmFeatureIds = [];

                // Procura diretamente no GeoJSON pela propriedade FUNDO_AGR
                const foundFeatures = geoJsonData.features.filter(feature => {
                    const fundoAgricola = this._findProp(feature, ['FUNDO_AGR']);
                    return fundoAgricola && String(fundoAgricola).toUpperCase().includes(searchTerm);
                });

                if (foundFeatures.length === 0) {
                    App.ui.showAlert(`Nenhum fundo agrícola encontrado com o termo "${searchInput.value}" no mapa.`, "info");
                    return;
                }

                // **HOTFIX** Pega os IDs das features encontradas
                const foundFeatureIds = foundFeatures.map(f => f.id);

                // **HOTFIX** Usa os IDs para consultar as features que estão na fonte do mapa (que têm o ID correto para setFeatureState)
                const sourceFeatures = mapboxMap.querySourceFeatures('talhoes-source', {
                    filter: ['in', ['id'], ...foundFeatureIds]
                });

                if (sourceFeatures.length === 0) {
                     App.ui.showAlert(`Nenhum fundo agrícola correspondente encontrado na fonte do mapa.`, "warning");
                    return;
                }

                const featureCollection = turf.featureCollection(sourceFeatures);
                const bbox = turf.bbox(featureCollection);
                const bounds = [[bbox[0], bbox[1]], [bbox[2], bbox[3]]];

                mapboxMap.fitBounds(bounds, {
                    padding: 60,
                    maxZoom: 14,
                    duration: 1500
                });

                const featureIdsToHighlight = sourceFeatures.map(f => f.id);
                featureIdsToHighlight.forEach(id => {
                    mapboxMap.setFeatureState({ source: 'talhoes-source', id: id }, { searched: true });
                });
                mapboxMap.searchedFarmFeatureIds = featureIdsToHighlight;

                // Remove o destaque após 8 segundos
                setTimeout(() => {
                    featureIdsToHighlight.forEach(id => {
                        if (mapboxMap.searchedFarmFeatureIds && mapboxMap.searchedFarmFeatureIds.includes(id)) {
                             mapboxMap.setFeatureState({ source: 'talhoes-source', id: id }, { searched: false });
                        }
                    });
                }, 8000);
            },
        },

        charts: {
            _getVibrantColors(count) {
                const colors = [
                    '#1976D2', '#D32F2F', '#388E3C', '#F57C00', '#7B1FA2', '#00796B',
                    '#C2185B', '#512DA8', '#FBC02D', '#FFA000', '#689F38', '#455A64'
                ];
                const result = [];
                for (let i = 0; i < count; i++) {
                    result.push(colors[i % colors.length]);
                }
                return result;
            },
            _getCommonChartOptions(options = {}) {
                const { hasLongLabels = false, indexAxis = 'x' } = options;
                const styles = getComputedStyle(document.documentElement);
                const isDarkTheme = document.body.classList.contains('theme-dark');
                
                const textColor = isDarkTheme ? '#FFFFFF' : styles.getPropertyValue('--color-text').trim();
                const borderColor = styles.getPropertyValue('--color-border').trim();

                const chartOptions = {
                    indexAxis: indexAxis,
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: {
                            grid: { 
                                display: false,
                                color: borderColor
                            },
                            ticks: { 
                                color: textColor,
                                autoSkip: !hasLongLabels,
                                maxRotation: hasLongLabels && indexAxis === 'x' ? 10 : 0,
                                minRotation: hasLongLabels && indexAxis === 'x' ? 10 : 0
                            }
                        },
                        y: {
                            grid: { 
                                display: false,
                                color: borderColor
                            },
                            ticks: { color: textColor },
                            grace: '10%'
                        }
                    },
                    plugins: {
                        legend: {
                            labels: {
                                color: textColor
                            }
                        }
                    }
                };

                return chartOptions;
            },
            _createOrUpdateChart(id, config, isExpanded = false) {
                const canvasId = isExpanded ? 'expandedChartCanvas' : id;
                const ctx = document.getElementById(canvasId)?.getContext('2d');
                if (!ctx) return;

                let chartInstance = isExpanded ? App.state.expandedChart : App.state.charts[id];

                if (chartInstance) {
                    // Otimização: Apenas atualiza os dados e a configuração em vez de destruir
                    chartInstance.data = config.data;
                    chartInstance.options = config.options;
                    chartInstance.update();
                } else {
                    // Cria um novo gráfico se não existir
                    const newChart = new Chart(ctx, config);
                    if (isExpanded) {
                        App.state.expandedChart = newChart;
                    } else {
                        App.state.charts[id] = newChart;
                    }
                }
            },
               destroyAll() {
                Object.keys(App.state.charts).forEach(id => {
                    if (App.state.charts[id]) {
                        App.state.charts[id].destroy();
                        delete App.state.charts[id];
                    }
                });
                if (App.state.expandedChart) {
                    App.state.expandedChart.destroy();
                    App.state.expandedChart = null;
                }
            },

            _createGaugeChart(canvasId, value, maxValue, label) {
                const percentage = maxValue > 0 ? (value / maxValue) * 100 : 0;
                const data = {
                    labels: [label, 'Restante'],
                    datasets: [{
                        data: [value, Math.max(0, maxValue - value)],
                        backgroundColor: [
                            '#4caf50',
                            '#e0e0e0'
                        ],
                        borderColor: [
                            '#4caf50',
                            '#e0e0e0'
                        ],
                        borderWidth: 1,
                        circumference: 180,
                        rotation: 270,
                    }]
                };

                const textCenter = {
                    id: 'textCenter',
                    beforeDatasetsDraw(chart, args, pluginOptions) {
                        const { ctx, data } = chart;
                        ctx.save();
                        ctx.font = 'bolder 30px Poppins';
                        ctx.fillStyle = '#4caf50';
                        ctx.textAlign = 'center';
                        ctx.textBaseline = 'middle';
                        const x = chart.getDatasetMeta(0).data[0].x;
                        const y = chart.getDatasetMeta(0).data[0].y - 15; // Move the text up
                        ctx.fillText(`${percentage.toFixed(1)}%`, x, y);
                    }
                }

                this._createOrUpdateChart(canvasId, {
                    type: 'doughnut',
                    data: data,
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: {
                                display: false
                            },
                            tooltip: {
                                enabled: false
                            },
                            datalabels: {
                                display: false
                            }
                        },
                        cutout: '70%'
                    },
                    plugins: [textCenter]
                });
            },

            openChartModal(chartId) {
                const originalChart = App.state.charts[chartId];
                if (!originalChart) return;

                const modal = App.elements.chartModal;
                const originalTitle = document.querySelector(`.chart-card [data-chart-id="${chartId}"]`).closest('.chart-card').querySelector('.chart-title').textContent;

                modal.title.textContent = originalTitle;
                modal.overlay.classList.add('show');

                // Cria uma cópia da configuração para não modificar o gráfico original
                const config = {
                    ...originalChart.config._config,
                    options: {
                        ...originalChart.config._config.options,
                        maintainAspectRatio: false
                    }
                };

                // Limpa o canvas anterior antes de criar um novo gráfico
                if (App.state.expandedChart) {
                    App.state.expandedChart.destroy();
                    App.state.expandedChart = null;
                }

                this._createOrUpdateChart(chartId, config, true);
            },
            closeChartModal() {
                const modal = App.elements.chartModal;
                modal.overlay.classList.remove('show');
                if (App.state.expandedChart) {
                    App.state.expandedChart.destroy();
                    App.state.expandedChart = null;
                }
            },

            
            async renderBrocaDashboardCharts() {
                const { brocaDashboardInicio, brocaDashboardFim } = App.elements.dashboard;
                App.actions.saveDashboardDates('broca', brocaDashboardInicio.value, brocaDashboardFim.value);
                const consolidatedData = await App.actions.getConsolidatedData('registros');
                const data = App.actions.filterDashboardData(consolidatedData, brocaDashboardInicio.value, brocaDashboardFim.value);

                // Otimização: Passar os dados já filtrados para as funções de renderização
                this.renderTop10FazendasBroca(data);
                this.renderBrocaMensal(data);
                this.renderBrocaPosicao(data);
                this.renderBrocaPorVariedade(data);
            },
            async renderPerdaDashboardCharts() {
                const { perdaDashboardInicio, perdaDashboardFim } = App.elements.dashboard;
                App.actions.saveDashboardDates('perda', perdaDashboardInicio.value, perdaDashboardFim.value);
                const consolidatedData = await App.actions.getConsolidatedData('perdas');
                const data = App.actions.filterDashboardData(consolidatedData, perdaDashboardInicio.value, perdaDashboardFim.value);

                // Otimização: Passar os dados já filtrados para as funções de renderização
                this.renderPerdaPorFrenteTurno(data);
                this.renderComposicaoPerdaPorFrente(data);
                this.renderTop10FazendasPerda(data);
                this.renderPerdaPorFrente(data);
            },
            renderTop10FazendasBroca(data) {
                const fazendasMap = new Map();
                data.forEach(item => {
                    const fazendaKey = `${item.codigo} - ${item.fazenda}`;
                    if (!fazendasMap.has(fazendaKey)) fazendasMap.set(fazendaKey, { totalEntrenos: 0, totalBrocado: 0 });
                    const f = fazendasMap.get(fazendaKey);
                    f.totalEntrenos += Number(item.entrenos);
                    f.totalBrocado += Number(item.brocado);
                });
                const fazendasArray = Array.from(fazendasMap.entries()).map(([nome, d]) => ({ nome, indice: d.totalEntrenos > 0 ? (d.totalBrocado / d.totalEntrenos) * 100 : 0 }));
                fazendasArray.sort((a, b) => b.indice - a.indice);
                const top10 = fazendasArray.slice(0, 10);
                
                const commonOptions = this._getCommonChartOptions({ hasLongLabels: true });
                const datalabelColor = document.body.classList.contains('theme-dark') ? '#FFFFFF' : '#333333';

                this._createOrUpdateChart('graficoTop10FazendasBroca', {
                    type: 'bar',
                    data: {
                        labels: top10.map(f => f.nome),
                        datasets: [{
                            label: 'Índice de Broca (%)',
                            data: top10.map(f => f.indice),
                            backgroundColor: this._getVibrantColors(top10.length)
                        }]
                    },
                    options: { 
                        ...commonOptions,
                        plugins: {
                            ...commonOptions.plugins,
                            legend: { display: false },
                            datalabels: {
                                color: datalabelColor, 
                                anchor: 'end', 
                                align: 'end',
                                font: { weight: 'bold', size: 14 },
                                formatter: (value) => `${value.toFixed(2)}%`
                            }
                        }
                    }
                });
            },
            renderBrocaMensal(data) {
                const dataByMonth = {};
                data.forEach(item => {
                    if (!item.data) return;
                    const date = new Date(item.data + 'T03:00:00Z');
                    const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
                    const monthLabel = date.toLocaleString('pt-BR', { month: 'short', year: '2-digit' });
                    if (!dataByMonth[monthKey]) dataByMonth[monthKey] = { totalBrocado: 0, totalEntrenos: 0, label: monthLabel };
                    dataByMonth[monthKey].totalBrocado += Number(item.brocado);
                    dataByMonth[monthKey].totalEntrenos += Number(item.entrenos);
                });
                const sortedMonths = Object.keys(dataByMonth).sort();
                const labels = sortedMonths.map(key => dataByMonth[key].label);
                const chartData = sortedMonths.map(key => {
                    const monthData = dataByMonth[key];
                    return monthData.totalEntrenos > 0 ? (monthData.totalBrocado / monthData.totalEntrenos) * 100 : 0;
                });
                
                const commonOptions = this._getCommonChartOptions();
                const datalabelColor = document.body.classList.contains('theme-dark') ? '#FFFFFF' : '#333333';

                this._createOrUpdateChart('graficoBrocaMensal', {
                    type: 'line',
                    data: {
                        labels,
                        datasets: [{
                            label: 'Índice Mensal (%)',
                            data: chartData,
                            fill: true,
                            borderColor: App.ui._getThemeColors().primary,
                            backgroundColor: 'rgba(54, 162, 235, 0.2)',
                            tension: 0.4
                        }]
                    },
                    options: { 
                        ...commonOptions,
                        scales: { 
                            ...commonOptions.scales,
                            y: { ...commonOptions.scales.y, grid: { color: 'transparent', drawBorder: false } } 
                        },
                        plugins: {
                            ...commonOptions.plugins,
                            legend: { display: false },
                            datalabels: {
                                anchor: 'end', align: 'top', offset: 8,
                                color: datalabelColor,
                                font: { weight: 'bold', size: 14 },
                                formatter: (value) => `${value.toFixed(2)}%`
                            }
                        }
                    }
                });
            },
            renderBrocaPosicao(data) {
                const totalBase = data.reduce((sum, item) => sum + Number(item.base), 0);
                const totalMeio = data.reduce((sum, item) => sum + Number(item.meio), 0);
                const totalTopo = data.reduce((sum, item) => sum + Number(item.topo), 0);
                const totalGeral = totalBase + totalMeio + totalTopo;
                
                const commonOptions = this._getCommonChartOptions();

                this._createOrUpdateChart('graficoBrocaPosicao', {
                    type: 'doughnut',
                    data: {
                        labels: ['Base', 'Meio', 'Topo'],
                        datasets: [{
                            label: 'Posição da Broca',
                            data: [totalBase, totalMeio, totalTopo],
                            backgroundColor: this._getVibrantColors(3)
                        }]
                    },
                    options: { 
                        responsive: true, maintainAspectRatio: false,
                        plugins: {
                            ...commonOptions.plugins,
                            legend: { ...commonOptions.plugins.legend, position: 'top' },
                            datalabels: {
                                color: '#FFFFFF', 
                                font: { weight: 'bold', size: 16 },
                                formatter: (value) => totalGeral > 0 ? `${(value / totalGeral * 100).toFixed(2)}%` : '0.00%'
                            }
                        }
                    }
                });
            },
            renderBrocaPorVariedade(data) {
                const variedadesMap = new Map();
                const fazendas = App.state.fazendas;

                data.forEach(item => {
                    const farm = fazendas.find(f => f.code === item.codigo);
                    const talhao = farm?.talhoes.find(t => t.name.toUpperCase() === item.talhao.toUpperCase());
                    const variedade = talhao?.variedade || 'N/A';

                    if (!variedadesMap.has(variedade)) {
                        variedadesMap.set(variedade, { totalEntrenos: 0, totalBrocado: 0 });
                    }
                    const v = variedadesMap.get(variedade);
                    v.totalEntrenos += Number(item.entrenos);
                    v.totalBrocado += Number(item.brocado);
                });

                const variedadesArray = Array.from(variedadesMap.entries())
                    .map(([nome, d]) => ({ nome, indice: d.totalEntrenos > 0 ? (d.totalBrocado / d.totalEntrenos) * 100 : 0 }))
                    .filter(v => v.nome !== 'N/A');
                    
                variedadesArray.sort((a, b) => b.indice - a.indice);
                const top10 = variedadesArray.slice(0, 10);

                const commonOptions = this._getCommonChartOptions({ indexAxis: 'y' });
                const datalabelColor = document.body.classList.contains('theme-dark') ? '#FFFFFF' : '#333333';

                this._createOrUpdateChart('graficoBrocaPorVariedade', {
                    type: 'bar',
                    data: {
                        labels: top10.map(v => v.nome),
                        datasets: [{
                            label: 'Índice de Broca (%)',
                            data: top10.map(v => v.indice),
                            backgroundColor: this._getVibrantColors(top10.length).reverse()
                        }]
                    },
                    options: {
                        ...commonOptions,
                        plugins: {
                            ...commonOptions.plugins,
                            legend: { display: false },
                            datalabels: {
                                color: datalabelColor, 
                                anchor: 'end', 
                                align: 'end',
                                font: { weight: 'bold', size: 14 },
                                formatter: (value) => `${value.toFixed(2)}%`
                            }
                        }
                    }
                });
            },
            renderPerdaPorFrenteTurno(data) {
                const structuredData = {};
                const frentes = [...new Set(data.map(p => p.frenteServico || 'N/A'))].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
                const turnos = [...new Set(data.map(p => p.turno || 'N/A'))].sort();

                turnos.forEach(turno => {
                    structuredData[turno] = {};
                    frentes.forEach(frente => {
                        structuredData[turno][frente] = { total: 0, count: 0 };
                    });
                });

                data.forEach(p => {
                    const frente = p.frenteServico || 'N/A';
                    const turno = p.turno || 'N/A';
                    if (structuredData[turno] && structuredData[turno][frente]) {
                        structuredData[turno][frente].total += p.total;
                        structuredData[turno][frente].count++;
                    }
                });

                const datasets = frentes.map((frente, index) => ({
                    label: `Frente ${frente}`,
                    data: turnos.map(turno => {
                        const d = structuredData[turno][frente];
                        return d.count > 0 ? d.total / d.count : 0;
                    }),
                    backgroundColor: this._getVibrantColors(frentes.length)[index]
                }));
                
                const commonOptions = this._getCommonChartOptions();
                const datalabelColor = document.body.classList.contains('theme-dark') ? '#FFFFFF' : '#333333';

                this._createOrUpdateChart('graficoPerdaPorFrenteTurno', {
                    type: 'bar',
                    data: { labels: turnos.map(t => `Turno ${t}`), datasets },
                    options: {
                        ...commonOptions,
                        scales: { 
                            ...commonOptions.scales,
                            y: { ...commonOptions.scales.y, title: { display: true, text: 'Perda Média (kg)', color: commonOptions.scales.y.ticks.color } } 
                        },
                        plugins: {
                            ...commonOptions.plugins,
                            datalabels: {
                                color: datalabelColor,
                                font: { weight: 'bold', size: 12 },
                                formatter: (value) => value > 0 ? `${value.toFixed(2)} kg` : ''
                            }
                        }
                    }
                });
            },
            renderComposicaoPerdaPorFrente(data) {
                const tiposDePerda = ['canaInteira', 'tolete', 'toco', 'ponta', 'estilhaco', 'pedaco'];
                const tiposLabels = ['C. Inteira', 'Tolete', 'Toco', 'Ponta', 'Estilhaço', 'Pedaço'];
                const frentes = [...new Set(data.map(p => p.frenteServico || 'N/A'))].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
                const structuredData = {};

                tiposDePerda.forEach(tipo => {
                    structuredData[tipo] = {};
                    frentes.forEach(frente => {
                        structuredData[tipo][frente] = 0;
                    });
                });

                data.forEach(item => {
                    const frente = item.frenteServico || 'N/A';
                    tiposDePerda.forEach(tipo => {
                        structuredData[tipo][frente] += item[tipo] || 0;
                    });
                });

                const datasets = frentes.map((frente, index) => ({
                    label: `Frente ${frente}`,
                    data: tiposDePerda.map(tipo => structuredData[tipo][frente]),
                    backgroundColor: this._getVibrantColors(frentes.length)[index]
                }));

                const commonOptions = this._getCommonChartOptions();
                
                this._createOrUpdateChart('graficoComposicaoPerda', {
                    type: 'bar',
                    data: { labels: tiposLabels, datasets },
                    options: {
                        ...commonOptions,
                        scales: { 
                            x: { ...commonOptions.scales.x, stacked: true }, 
                            y: { ...commonOptions.scales.y, stacked: true, title: { display: true, text: 'Perda Total (kg)', color: commonOptions.scales.y.ticks.color } } 
                        },
                        plugins: {
                             ...commonOptions.plugins,
                             datalabels: {
                                color: '#FFFFFF',
                                font: { weight: 'bold' },
                                formatter: (value) => value > 0.1 ? `${value.toFixed(2)} kg` : ''
                            }
                        }
                    }
                });
            },
            renderTop10FazendasPerda(data) {
                const fazendas = {};
                data.forEach(item => {
                    const fazendaKey = `${item.codigo} - ${item.fazenda}`;
                    if (!fazendas[fazendaKey]) fazendas[fazendaKey] = { total: 0, count: 0 };
                    fazendas[fazendaKey].total += item.total;
                    fazendas[fazendaKey].count++;
                });
                const sortedFazendas = Object.entries(fazendas)
                    .map(([nome, data]) => ({ nome, media: data.count > 0 ? data.total / data.count : 0 }))
                    .sort((a, b) => b.media - a.media).slice(0, 10);

                const commonOptions = this._getCommonChartOptions({ hasLongLabels: true });
                const datalabelColor = document.body.classList.contains('theme-dark') ? '#FFFFFF' : '#333333';

                this._createOrUpdateChart('graficoTop10FazendasPerda', {
                    type: 'bar',
                    data: {
                        labels: sortedFazendas.map(f => f.nome),
                        datasets: [{
                            label: 'Perda Média (kg)',
                            data: sortedFazendas.map(f => f.media),
                            backgroundColor: this._getVibrantColors(sortedFazendas.length)
                        }]
                    },
                    options: {
                        ...commonOptions,
                        plugins: {
                            ...commonOptions.plugins,
                            legend: { display: false },
                            datalabels: {
                                color: datalabelColor, 
                                anchor: 'end', 
                                align: 'end',
                                font: { weight: 'bold', size: 14 },
                                formatter: (value) => `${value.toFixed(2)} kg`
                            }
                        }
                    }
                });
            },
            renderPerdaPorFrente(data) {
                const frentes = {};
                data.forEach(item => {
                    const frente = item.frenteServico || 'N/A';
                    if (!frentes[frente]) frentes[frente] = { total: 0, count: 0 };
                    frentes[frente].total += item.total;
                    frentes[frente].count++;
                });
                const sortedFrentes = Object.entries(frentes)
                    .map(([nome, data]) => ({ nome: `Frente ${nome}`, media: data.count > 0 ? data.total / data.count : 0 }))
                    .sort((a, b) => a.nome.localeCompare(b.nome, undefined, { numeric: true }));

                const commonOptions = this._getCommonChartOptions();
                const datalabelColor = document.body.classList.contains('theme-dark') ? '#FFFFFF' : '#333333';

                this._createOrUpdateChart('graficoPerdaPorFrente', {
                    type: 'bar',
                    data: {
                        labels: sortedFrentes.map(f => f.nome),
                        datasets: [{
                            label: 'Perda Média (kg)',
                            data: sortedFrentes.map(f => f.media),
                            backgroundColor: this._getVibrantColors(sortedFrentes.length)
                        }]
                    },
                    options: {
                        ...commonOptions,
                        plugins: {
                            ...commonOptions.plugins,
                            legend: { display: false },
                            datalabels: {
                                color: datalabelColor, 
                                anchor: 'end', 
                                align: 'end',
                                font: { weight: 'bold', size: 14 },
                                formatter: (value) => `${value.toFixed(2)} kg`
                            }
                        }
                    }
                });
            },

            async renderPlantioDashboardCharts() {
                const startDateEl = document.getElementById('plantioDashboardInicio');
                const endDateEl = document.getElementById('plantioDashboardFim');
                const culturaEl = document.getElementById('plantioDashboardCultura');
                App.actions.saveDashboardDates('plantio', startDateEl.value, endDateEl.value);

                const consolidatedData = await App.actions.getConsolidatedData('apontamentosPlantio');
                let data = App.actions.filterDashboardData(consolidatedData, startDateEl.value, endDateEl.value);

                const selectedCulture = culturaEl.value;
                if (selectedCulture) {
                    data = data.filter(item => item.culture === selectedCulture);
                }

                const normalizeCultureKey = (cultureString) => {
                    if (!cultureString) return '';
                    // Converts to uppercase, removes accents, and then removes any character that is not A-Z or 0-9.
                    return cultureString.toUpperCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[^A-Z0-9]/g, '');
                };


                // 1. Calculate KPIs
                const totalAreaPlantada = data.reduce((sum, item) => sum + item.totalArea, 0);

                // Load the correct goal based on the selected culture
                const plantingGoals = App.state.companyConfig.plantingGoals || {};
                let totalGoal = 0;
                let dailyGoalTotal = 0;

                if (selectedCulture) {
                    const normalizedKey = normalizeCultureKey(selectedCulture);
                    const goalData = plantingGoals[normalizedKey];
                    if (goalData) {
                        totalGoal = goalData.total || 0;
                        dailyGoalTotal = goalData.daily || 0;
                    }
                } else {
                    // Se "Todas" estiver selecionado, soma as metas de todas as culturas
                    for (const culture in plantingGoals) {
                        totalGoal += plantingGoals[culture].total || 0;
                        dailyGoalTotal += plantingGoals[culture].daily || 0;
                    }
                }
                if (totalGoal === 0 && !selectedCulture) totalGoal = 1000; // Fallback

                const percentualConcluido = totalGoal > 0 ? (totalAreaPlantada / totalGoal) * 100 : 0;
                const areaRestante = totalGoal - totalAreaPlantada;

                const plantingDays = new Set(data.map(item => item.date));
                const numberOfPlantingDays = plantingDays.size;
                const mediaDiariaReal = numberOfPlantingDays > 0 ? totalAreaPlantada / numberOfPlantingDays : 0;


                // 2. Update KPI elements
                document.getElementById('kpi-plantio-area-total').textContent = `${totalAreaPlantada.toFixed(2)} ha`;
                document.getElementById('kpi-plantio-meta').textContent = `${totalGoal.toFixed(2)} ha`;
                document.getElementById('kpi-plantio-percentual').textContent = `${percentualConcluido.toFixed(1)}%`;
                document.getElementById('kpi-plantio-media-diaria').textContent = `${mediaDiariaReal.toFixed(2)} ha/dia`;
                document.getElementById('kpi-plantio-area-restante').textContent = `${areaRestante.toFixed(2)} ha`;

                // 3. Render Charts
                this.renderAreaPlantadaPorMes(data);
                this.renderProdutividadePorFrente(data);
                this.renderEvolucaoAreaPlantada(data);
                this.renderConclusaoPlantio(totalAreaPlantada, totalGoal);
                this.renderChuvaPorFazenda(data);
            },

            renderChuvaPorFazenda(data) {
                const dataByFarm = data.reduce((acc, item) => {
                    const farmName = item.farmName || 'N/A';
                    if (!acc[farmName]) {
                        acc[farmName] = { totalChuva: 0, count: 0 };
                    }
                    if (item.chuva && !isNaN(parseFloat(item.chuva))) {
                        acc[farmName].totalChuva += parseFloat(item.chuva);
                        acc[farmName].count++;
                    }
                    return acc;
                }, {});

                const sortedFarms = Object.entries(dataByFarm)
                    .map(([name, { totalChuva, count }]) => ({
                        name,
                        avgChuva: count > 0 ? totalChuva / count : 0
                    }))
                    .sort((a, b) => b.avgChuva - a.avgChuva);

                const labels = sortedFarms.map(item => item.name);
                const chartData = sortedFarms.map(item => item.avgChuva);

                const commonOptions = this._getCommonChartOptions();
                const datalabelColor = document.body.classList.contains('theme-dark') ? '#FFFFFF' : '#333333';

                this._createOrUpdateChart('graficoChuvaPorFazenda', {
                    type: 'bar',
                    data: {
                        labels,
                        datasets: [{
                            label: 'Média de Chuva (mm)',
                            data: chartData,
                            backgroundColor: this._getVibrantColors(labels.length),
                        }]
                    },
                    options: {
                        ...commonOptions,
                        plugins: {
                            ...commonOptions.plugins,
                            legend: { display: false },
                            datalabels: {
                                color: datalabelColor,
                                anchor: 'end',
                                align: 'top',
                                font: { weight: 'bold' },
                                formatter: value => `${value.toFixed(1)} mm`
                            }
                        }
                    }
                });
            },

            renderAreaPlantadaPorMes(data) { // Renamed from renderAreaPlantadaPorDia and logic updated
                const dataByMonth = data.reduce((acc, item) => {
                    const date = new Date(item.date + 'T03:00:00Z');
                    const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
                    acc[monthKey] = (acc[monthKey] || 0) + item.totalArea;
                    return acc;
                }, {});

                const sortedMonths = Object.keys(dataByMonth).sort();
                const labels = sortedMonths.map(monthKey => {
                    const [year, month] = monthKey.split('-');
                    return new Date(year, month - 1, 1).toLocaleString('pt-BR', { month: 'short', year: '2-digit' });
                });
                const chartData = sortedMonths.map(monthKey => dataByMonth[monthKey]);
                const totalPlantedInRange = chartData.reduce((sum, value) => sum + value, 0);

                const commonOptions = this._getCommonChartOptions();
                const datalabelColor = document.body.classList.contains('theme-dark') ? '#FFFFFF' : '#333333';

                this._createOrUpdateChart('graficoAreaPlantadaPorDia', { // Canvas ID is still the same
                    type: 'bar',
                    data: {
                        labels,
                        datasets: [{
                            label: 'Área Plantada (ha)',
                            data: chartData,
                            backgroundColor: '#4caf50',
                        }]
                    },
                    options: {
                        ...commonOptions,
                         plugins: {
                            ...commonOptions.plugins,
                            legend: { display: false },
                            datalabels: {
                                color: datalabelColor,
                                anchor: 'end',
                                align: 'top',
                                font: { weight: 'bold' },
                                formatter: (value) => {
                                    const percentage = totalPlantedInRange > 0 ? (value / totalPlantedInRange) * 100 : 0;
                                    return `${value.toFixed(2)} ha (${percentage.toFixed(1)}%)`;
                                }
                            }
                        }
                    }
                });
            },

            renderProdutividadePorFrente(data) {
                const dataByFrente = data.reduce((acc, item) => {
                    const frente = item.frenteDePlantioName || 'N/A';
                    acc[frente] = (acc[frente] || 0) + item.totalArea;
                    return acc;
                }, {});

                const sortedFrentes = Object.entries(dataByFrente).sort(([, a], [, b]) => b - a);
                const labels = sortedFrentes.map(([name]) => name);
                const chartData = sortedFrentes.map(([, total]) => total);

                const commonOptions = this._getCommonChartOptions();
                 const datalabelColor = document.body.classList.contains('theme-dark') ? '#FFFFFF' : '#333333';

                this._createOrUpdateChart('graficoProdutividadePorFrente', {
                    type: 'bar',
                    data: {
                        labels,
                        datasets: [{
                            label: 'Área Plantada (ha)',
                            data: chartData,
                            backgroundColor: this._getVibrantColors(labels.length),
                        }]
                    },
                    options: {
                        ...commonOptions,
                        plugins: {
                            ...commonOptions.plugins,
                            legend: { display: false },
                            datalabels: {
                                color: datalabelColor,
                                anchor: 'end',
                                align: 'top',
                                font: { weight: 'bold' },
                                formatter: value => `${value.toFixed(2)} ha`
                            }
                        }
                    }
                });
            },

            renderEvolucaoAreaPlantada(data) {
                const dataByDay = data.reduce((acc, item) => {
                    const date = item.date;
                    acc[date] = (acc[date] || 0) + item.totalArea;
                    return acc;
                }, {});

                const sortedDays = Object.keys(dataByDay).sort();
                const labels = sortedDays.map(date => new Date(date + 'T03:00:00Z').toLocaleDateString('pt-BR'));

                let cumulativeTotal = 0;
                const cumulativeData = sortedDays.map(date => {
                    cumulativeTotal += dataByDay[date];
                    return cumulativeTotal;
                });

                // Lógica de cálculo da meta diária
                const plantingGoals = App.state.companyConfig.plantingGoals || {};
                const selectedCulture = document.getElementById('plantioDashboardCultura').value;
                const normalizeCultureKey = (cultureString) => {
                    if (!cultureString) return '';
                    return cultureString.toUpperCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[^A-Z0-9]/g, '');
                };

                let dailyGoal = 0;
                let totalGoal = 0; // Usado para limitar a linha da meta

                if (selectedCulture) {
                    const normalizedKey = normalizeCultureKey(selectedCulture);
                    const goalData = plantingGoals[normalizedKey];
                    if (goalData) {
                        dailyGoal = goalData.daily || 0;
                        totalGoal = goalData.total || 0;
                    }
                } else {
                    // Se "Todas" estiver selecionado, soma as metas diárias de todas as culturas
                    dailyGoal = Object.values(plantingGoals).reduce((sum, goal) => sum + (goal.daily || 0), 0);
                    totalGoal = Object.values(plantingGoals).reduce((sum, goal) => sum + (goal.total || 0), 0);
                }
                 if (totalGoal === 0 && !selectedCulture) totalGoal = 1000;


                let cumulativeGoal = 0;
                const cumulativeGoalData = sortedDays.map(() => {
                    if (dailyGoal > 0 && cumulativeGoal < totalGoal) {
                        cumulativeGoal = Math.min(totalGoal, cumulativeGoal + dailyGoal);
                    } else if (dailyGoal > 0) {
                        cumulativeGoal = totalGoal; // Trava a linha no total da meta
                    }
                    return cumulativeGoal;
                });


                const commonOptions = this._getCommonChartOptions();

                this._createOrUpdateChart('graficoEvolucaoAreaPlantada', {
                    type: 'line',
                    data: {
                        labels,
                        datasets: [
                        {
                            label: 'Área Acumulada (ha)',
                            data: cumulativeData,
                            fill: true,
                            borderColor: '#1976d2',
                            backgroundColor: 'rgba(25, 118, 210, 0.2)',
                            tension: 0.3
                        },
                        {
                            label: 'Meta Acumulada (ha)',
                            data: cumulativeGoalData,
                            fill: false,
                            borderColor: '#d32f2f',
                            borderDash: [5, 5],
                            tension: 0.3
                        }
                    ]
                    },
                     options: {
                        ...commonOptions,
                         plugins: {
                            ...commonOptions.plugins,
                            legend: { display: true },
                            tooltip: {
                                callbacks: {
                                    label: function(tooltipItem) {
                                        const dataIndex = tooltipItem.dataIndex;
                                        // Se for o gráfico de "Área Acumulada"
                                        if (tooltipItem.datasetIndex === 0) {
                                            const dateKey = sortedDays[dataIndex];
                                            const dailyValue = dataByDay[dateKey] || 0;
                                            const cumulativeValue = tooltipItem.parsed.y || 0;

                                            const dailyLabel = `Área do Dia: ${dailyValue.toFixed(2).replace('.', ',')} ha`;
                                            const cumulativeLabel = `Acumulado: ${cumulativeValue.toFixed(2).replace('.', ',')} ha`;

                                            return [dailyLabel, cumulativeLabel];
                                        }
                                        // Comportamento padrão para outros gráficos (linha da meta)
                                        let label = tooltipItem.dataset.label || '';
                                        if (label) {
                                            label += ': ';
                                        }
                                        if (tooltipItem.parsed.y !== null) {
                                            label += tooltipItem.parsed.y.toFixed(2).replace('.', ',') + ' ha';
                                        }
                                        return label;
                                    }
                                }
                            },
                            datalabels: {
                                display: function(context) {
                                    // Only display the label for the last data point
                                    return context.dataIndex === context.dataset.data.length - 1;
                                },
                                anchor: 'end',
                                align: 'top',
                                offset: 8,
                                borderRadius: 4,
                                color: 'white',
                                font: {
                                    weight: 'bold'
                                },
                                padding: 6,
                                formatter: (value) => {
                                    return value.toFixed(1).replace('.', ',') + ' ha';
                                },
                                backgroundColor: function(context) {
                                    return context.datasetIndex === 0 ? 'rgba(25, 118, 210, 0.8)' : 'rgba(211, 47, 47, 0.8)';
                                }
                            }
                        }
                    }
                });
            },

            renderConclusaoPlantio(totalArea, meta) {
                this._createGaugeChart('graficoConclusaoPlantio', totalArea, meta, 'Concluído');
            },

            async renderAereoDashboardCharts() {
                const startDateEl = document.getElementById('aereoDashboardInicio');
                const endDateEl = document.getElementById('aereoDashboardFim');
                App.actions.saveDashboardDates('aereo', startDateEl.value, endDateEl.value);

                const consolidatedData = await App.actions.getConsolidatedData('armadilhas');
                const collectedTraps = consolidatedData.filter(t => t.status === 'Coletada');
                const data = App.actions.filterDashboardData(collectedTraps, startDateEl.value, endDateEl.value);

                const farmRiskData = this._calculateRiskForEachFarm(data);

                // KPIs
                const fazendasMonitoradas = farmRiskData.length;
                const fazendasEmRisco = farmRiskData.filter(f => f.riskPercentage > 30).length;
                const percentualFazendasRisco = fazendasMonitoradas > 0 ? (fazendasEmRisco / fazendasMonitoradas) * 100 : 0;
                const totalMariposas = data.reduce((sum, trap) => sum + (trap.contagemMariposas || 0), 0);
                const infestacaoMedia = data.length > 0 ? totalMariposas / data.length : 0;

                document.getElementById('kpi-aereo-fazendas-monitoradas').textContent = fazendasMonitoradas;
                document.getElementById('kpi-aereo-fazendas-risco').textContent = `${percentualFazendasRisco.toFixed(1)}%`;
                document.getElementById('kpi-aereo-infestacao-media').textContent = `${infestacaoMedia.toFixed(1)}`;
                document.getElementById('kpi-aereo-custo-total').textContent = 'R$ 0,00'; // Placeholder

                // Charts
                this.renderFazendasRiscoChart(fazendasMonitoradas, fazendasEmRisco);
                this.renderRiscoPorFazendaChart(farmRiskData);
                this.renderTendenciaAereaChart(data);
                this.renderCustoAereoChart(); // Placeholder chart
            },

            _calculateRiskForEachFarm(data) {
                const farms = App.state.fazendas;
                const farmRiskData = [];

                const trapsByFarm = data.reduce((acc, trap) => {
                    const farmCode = trap.fazendaCode;
                    if (!acc[farmCode]) {
                        acc[farmCode] = [];
                    }
                    acc[farmCode].push(trap);
                    return acc;
                }, {});


                for (const farmCode in trapsByFarm) {
                    const farmTraps = trapsByFarm[farmCode];
                    const farmInfo = farms.find(f => String(f.code) === String(farmCode));

                    if (farmTraps.length > 0) {
                        const highCountTraps = farmTraps.filter(t => t.contagemMariposas >= 6).length;
                        const riskPercentage = (highCountTraps / farmTraps.length) * 100;
                        farmRiskData.push({
                            farmName: farmInfo ? `${farmInfo.code} - ${farmInfo.name}`: `Fazenda ${farmCode}`,
                            riskPercentage: riskPercentage
                        });
                    }
                }

                return farmRiskData.sort((a, b) => b.riskPercentage - a.riskPercentage);
            },

            renderFazendasRiscoChart(totalFarms, riskFarms) {
                const commonOptions = this._getCommonChartOptions();
                const labels = ['Em Risco (>30%)', 'Fora de Risco'];
                 this._createOrUpdateChart('graficoFazendasRisco', {
                    type: 'doughnut',
                    data: {
                        labels: labels,
                        datasets: [{
                            label: 'Proporção de Fazendas',
                            data: [riskFarms, totalFarms - riskFarms],
                            backgroundColor: ['#d32f2f', '#388e3c'],
                        }]
                    },
                    options: {
                        ...commonOptions,
                        plugins: {
                             ...commonOptions.plugins,
                            datalabels: {
                                color: '#FFFFFF',
                                font: { weight: 'bold', size: 16 },
                                formatter: (value, context) => {
                                    const total = context.chart.data.datasets[0].data.reduce((a, b) => a + b, 0);
                                    const percentage = total > 0 ? (value / total * 100) : 0;
                                    return `${percentage.toFixed(1)}%`;
                                }
                            }
                        }
                    }
                });
            },

            renderRiscoPorFazendaChart(farmRiskData) {
                const top10Farms = farmRiskData.slice(0, 10);
                const labels = top10Farms.map(f => f.farmName);
                const data = top10Farms.map(f => f.riskPercentage);

                const commonOptions = this._getCommonChartOptions({hasLongLabels: true});
                const datalabelColor = document.body.classList.contains('theme-dark') ? '#FFFFFF' : '#333333';

                this._createOrUpdateChart('graficoRiscoPorFazenda', {
                    type: 'bar',
                    data: {
                        labels,
                        datasets: [{
                            label: 'Nível de Risco (%)',
                            data,
                            backgroundColor: data.map(risk => risk > 30 ? '#d32f2f' : '#388e3c'),
                        }]
                    },
                    options: {
                        ...commonOptions,
                        plugins: {
                             ...commonOptions.plugins,
                             legend: { display: false },
                             datalabels: {
                                 color: datalabelColor,
                                 anchor: 'end',
                                 align: 'top',
                                 font: { weight: 'bold' },
                                 formatter: value => `${value.toFixed(1)}%`
                             }
                        }
                    }
                });
            },

            renderTendenciaAereaChart(data) {
                const dataByDay = data.reduce((acc, trap) => {
                    const date = trap.dataColeta?.toDate ? trap.dataColeta.toDate().toISOString().split('T')[0] : new Date(trap.dataColeta).toISOString().split('T')[0];
                    if (!acc[date]) {
                        acc[date] = { totalMoths: 0, count: 0 };
                    }
                    acc[date].totalMoths += trap.contagemMariposas || 0;
                    acc[date].count++;
                    return acc;
                }, {});

                const sortedDays = Object.keys(dataByDay).sort();
                const labels = sortedDays.map(date => new Date(date + 'T03:00:00Z').toLocaleDateString('pt-BR'));
                const chartData = sortedDays.map(date => dataByDay[date].totalMoths / dataByDay[date].count);

                const commonOptions = this._getCommonChartOptions();

                this._createOrUpdateChart('graficoTendenciaAerea', {
                    type: 'line',
                    data: {
                        labels,
                        datasets: [{
                            label: 'Média de Mariposas / Dia',
                            data: chartData,
                            borderColor: '#f57c00',
                            backgroundColor: 'rgba(245, 124, 0, 0.2)',
                            fill: true,
                            tension: 0.3
                        }]
                    },
                    options: {
                        ...commonOptions,
                         plugins: {
                             ...commonOptions.plugins,
                            legend: { display: false },
                            datalabels: { display: false }
                        }
                    }
                });
            },

            renderCustoAereoChart() {
                // Placeholder chart
                const commonOptions = this._getCommonChartOptions();
                 this._createOrUpdateChart('graficoCustoAereo', {
                    type: 'bar',
                    data: {
                        labels: ['Fazenda A', 'Fazenda B', 'Fazenda C'],
                        datasets: [{
                            label: 'Custo Total',
                            data: [1200, 1900, 800],
                            backgroundColor: '#1976d2'
                        }, {
                            label: 'Custo Médio',
                            data: [300, 475, 200],
                            backgroundColor: '#689f38'
                        }]
                    },
                    options: {
                        ...commonOptions,
                         scales: { ...commonOptions.scales, x: { ...commonOptions.scales.x, stacked: true }, y: { ...commonOptions.scales.y, stacked: true } }
                    }
                });
            },

            async renderCigarrinhaDashboardCharts() {
                const startDateEl = document.getElementById('cigarrinhaDashboardInicio');
                const endDateEl = document.getElementById('cigarrinhaDashboardFim');
                App.actions.saveDashboardDates('cigarrinha', startDateEl.value, endDateEl.value);

                const cigarrinhaData = await App.actions.getConsolidatedData('cigarrinha');
                const amostragemData = await App.actions.getConsolidatedData('cigarrinhaAmostragem');
                const combinedData = [...cigarrinhaData, ...amostragemData];
                const data = App.actions.filterDashboardData(combinedData, startDateEl.value, endDateEl.value);

                const dataByTalhao = data.reduce((acc, item) => {
                    const talhaoKey = `${item.codigo}-${item.talhao}`;
                    if (!acc[talhaoKey]) {
                        acc[talhaoKey] = [];
                    }
                    acc[talhaoKey].push(item.resultado);
                    return acc;
                }, {});

                const talhaoAverages = Object.entries(dataByTalhao).map(([key, results]) => {
                    const avg = results.reduce((sum, r) => sum + r, 0) / results.length;
                    return { talhao: key, avgResult: avg };
                });

                // KPIs
                const totalResultado = talhaoAverages.reduce((sum, t) => sum + t.avgResult, 0);
                const infestacaoMedia = talhaoAverages.length > 0 ? totalResultado / talhaoAverages.length : 0;
                const talhoesAvaliados = talhaoAverages.length;
                const talhoesEmAlerta = talhaoAverages.filter(t => t.avgResult >= 2.0).length;

                document.getElementById('kpi-cigarrinha-infestacao-media').textContent = infestacaoMedia.toFixed(2);
                document.getElementById('kpi-cigarrinha-talhoes-avaliados').textContent = talhoesAvaliados;
                document.getElementById('kpi-cigarrinha-eficiencia-controle').textContent = 'N/A';
                document.getElementById('kpi-cigarrinha-talhoes-alerta').textContent = talhoesEmAlerta;

                // Charts
                this.renderInfestacaoMediaTalhao(talhaoAverages);
                this.renderTendenciaCigarrinha(data);
                this.renderEficienciaControleChart(); // Placeholder
                this.renderNivelInfestacaoChart(talhaoAverages);
            },

            renderInfestacaoMediaTalhao(talhaoAverages) {
                const sortedData = [...talhaoAverages].sort((a, b) => b.avgResult - a.avgResult).slice(0, 15);
                const labels = sortedData.map(item => item.talhao);
                const data = sortedData.map(item => item.avgResult);

                const getBarColor = (value) => {
                    if (value >= 2.0) return '#d32f2f'; // Red for high
                    if (value >= 1.0) return '#f57c00'; // Orange for medium
                    return '#388e3c'; // Green for low
                };

                const commonOptions = this._getCommonChartOptions({ indexAxis: 'y', hasLongLabels: true });
                const datalabelColor = document.body.classList.contains('theme-dark') ? '#FFFFFF' : '#333333';

                this._createOrUpdateChart('graficoInfestacaoMediaTalhao', {
                    type: 'bar',
                    data: {
                        labels,
                        datasets: [{
                            label: 'Infestação Média',
                            data,
                            backgroundColor: data.map(getBarColor)
                        }]
                    },
                    options: {
                        ...commonOptions,
                        plugins: {
                            ...commonOptions.plugins,
                            legend: { display: false },
                             datalabels: {
                                color: datalabelColor,
                                anchor: 'end',
                                align: 'end',
                                font: { weight: 'bold' },
                                formatter: value => value.toFixed(2)
                            }
                        }
                    }
                });
            },

            renderTendenciaCigarrinha(data) {
                const dataByDay = data.reduce((acc, item) => {
                    const date = item.data;
                    if (!acc[date]) {
                        acc[date] = [];
                    }
                    acc[date].push(item.resultado);
                    return acc;
                }, {});

                const sortedDays = Object.keys(dataByDay).sort();
                const labels = sortedDays.map(date => new Date(date + 'T03:00:00Z').toLocaleDateString('pt-BR'));
                const chartData = sortedDays.map(date => {
                    const results = dataByDay[date];
                    return results.reduce((sum, r) => sum + r, 0) / results.length;
                });

                const commonOptions = this._getCommonChartOptions();

                this._createOrUpdateChart('graficoTendenciaCigarrinha', {
                    type: 'line',
                    data: {
                        labels,
                        datasets: [{
                            label: 'Infestação Média Diária',
                            data: chartData,
                            borderColor: '#7b1fa2',
                            backgroundColor: 'rgba(123, 31, 162, 0.2)',
                            fill: true,
                            tension: 0.4 // For smooth area chart
                        }]
                    },
                     options: {
                        ...commonOptions,
                        plugins: {
                            ...commonOptions.plugins,
                            legend: { display: false },
                            datalabels: { display: false }
                        }
                    }
                });
            },

            renderEficienciaControleChart() {
                 const commonOptions = this._getCommonChartOptions();
                 this._createOrUpdateChart('graficoEficienciaControle', {
                    type: 'bar',
                    data: {
                        labels: ['Talhão A', 'Talhão B', 'Talhão C', 'Talhão D'],
                        datasets: [{
                            label: 'Antes da Aplicação',
                            data: [2.5, 3.1, 1.8, 2.9],
                            backgroundColor: '#f57c00'
                        }, {
                            label: 'Depois da Aplicação',
                            data: [0.8, 1.0, 0.5, 1.2],
                            backgroundColor: '#388e3c'
                        }]
                    },
                    options: commonOptions
                });
            },

            renderNivelInfestacaoChart(talhaoAverages) {
                const niveis = {
                    'Baixa (<1.0)': 0,
                    'Média (1.0-1.9)': 0,
                    'Alta (>=2.0)': 0
                };

                talhaoAverages.forEach(item => {
                    if (item.avgResult >= 2.0) {
                        niveis['Alta (>=2.0)']++;
                    } else if (item.avgResult >= 1.0) {
                        niveis['Média (1.0-1.9)']++;
                    } else {
                        niveis['Baixa (<1.0)']++;
                    }
                });

                const commonOptions = this._getCommonChartOptions();

                this._createOrUpdateChart('graficoNivelInfestacao', {
                    type: 'doughnut',
                    data: {
                        labels: Object.keys(niveis),
                        datasets: [{
                            data: Object.values(niveis),
                            backgroundColor: ['#388e3c', '#f57c00', '#d32f2f']
                        }]
                    },
                     options: {
                        ...commonOptions,
                        plugins: {
                             ...commonOptions.plugins,
                            datalabels: {
                                color: '#FFFFFF',
                                font: { weight: 'bold', size: 16 },
                                formatter: (value, context) => {
                                    const total = context.chart.data.datasets[0].data.reduce((a, b) => a + b, 0);
                                    const percentage = total > 0 ? (value / total * 100) : 0;
                                     return percentage > 0 ? `${percentage.toFixed(1)}%` : '';
                                }
                            }
                        }
                    }
                });
            },

            async renderClimaDashboardCharts() {
                const startDateEl = document.getElementById('climaDashboardInicio');
                const endDateEl = document.getElementById('climaDashboardFim');
                const fazendaEl = document.getElementById('climaDashboardFazenda');

                App.actions.saveDashboardDates('clima', startDateEl.value, endDateEl.value);

                const consolidatedData = await App.actions.getConsolidatedData('clima');
                let data = App.actions.filterDashboardData(consolidatedData, startDateEl.value, endDateEl.value);

                const selectedFazenda = fazendaEl.value;
                if (selectedFazenda) {
                    data = data.filter(item => item.fazendaId === selectedFazenda);
                }

                // KPIs
                const avgTempMax = data.length > 0 ? data.reduce((sum, item) => sum + item.tempMax, 0) / data.length : 0;
                const avgTempMin = data.length > 0 ? data.reduce((sum, item) => sum + item.tempMin, 0) / data.length : 0;
                const totalPluviosidade = data.reduce((sum, item) => sum + item.pluviosidade, 0);
                const avgUmidade = data.length > 0 ? data.reduce((sum, item) => sum + item.umidade, 0) / data.length : 0;
                const avgVento = data.length > 0 ? data.reduce((sum, item) => sum + item.vento, 0) / data.length : 0;

                document.getElementById('kpi-clima-temp-max').textContent = `${avgTempMax.toFixed(1)}°C`;
                document.getElementById('kpi-clima-temp-min').textContent = `${avgTempMin.toFixed(1)}°C`;
                document.getElementById('kpi-clima-pluviosidade').textContent = `${totalPluviosidade.toFixed(1)} mm`;
                document.getElementById('kpi-clima-umidade').textContent = `${avgUmidade.toFixed(1)}%`;
                document.getElementById('kpi-clima-vento').textContent = `${avgVento.toFixed(1)} km/h`;

                // Render Charts
                this.renderVariacaoTemperaturaChart(data);
                this.renderAcumuloPluviosidadeChart(data);
                this.renderVelocidadeVentoChart(data);
                this.renderIndiceClimatologicoChart(data);
            },

            renderVariacaoTemperaturaChart(data) {
                const dataByDay = data.reduce((acc, item) => {
                    acc[item.data] = acc[item.data] || { tempsMax: [], tempsMin: [] };
                    acc[item.data].tempsMax.push(item.tempMax);
                    acc[item.data].tempsMin.push(item.tempMin);
                    return acc;
                }, {});

                const sortedDays = Object.keys(dataByDay).sort();
                const labels = sortedDays.map(date => new Date(date + 'T03:00:00Z').toLocaleDateString('pt-BR'));
                const avgMaxTemps = sortedDays.map(date => dataByDay[date].tempsMax.reduce((a, b) => a + b, 0) / dataByDay[date].tempsMax.length);
                const avgMinTemps = sortedDays.map(date => dataByDay[date].tempsMin.reduce((a, b) => a + b, 0) / dataByDay[date].tempsMin.length);

                const commonOptions = this._getCommonChartOptions();
                this._createOrUpdateChart('graficoVariacaoTemperatura', {
                    type: 'line',
                    data: {
                        labels,
                        datasets: [{
                            label: 'Temp. Máxima (°C)',
                            data: avgMaxTemps,
                            borderColor: '#D32F2F',
                            backgroundColor: 'rgba(211, 47, 47, 0.1)',
                            fill: true,
                            tension: 0.4
                        }, {
                            label: 'Temp. Mínima (°C)',
                            data: avgMinTemps,
                            borderColor: '#1976D2',
                            backgroundColor: 'rgba(25, 118, 210, 0.1)',
                            fill: true,
                            tension: 0.4
                        }]
                    },
                    options: {
                        ...commonOptions,
                        plugins: {
                            ...commonOptions.plugins,
                            datalabels: {
                                align: 'end',
                                anchor: 'end',
                                backgroundColor: (context) => context.dataset.backgroundColor || 'rgba(0, 0, 0, 0.8)',
                                borderRadius: 4,
                                color: 'white',
                                font: {
                                    weight: 'bold'
                                },
                                formatter: (value) => `${value.toFixed(1)}°C`,
                                padding: 6
                            }
                        }
                    }
                });
            },

            renderAcumuloPluviosidadeChart(data) {
                const dataByDay = data.reduce((acc, item) => {
                    acc[item.data] = (acc[item.data] || 0) + item.pluviosidade;
                    return acc;
                }, {});

                const sortedDays = Object.keys(dataByDay).sort();
                const labels = sortedDays.map(date => new Date(date + 'T03:00:00Z').toLocaleDateString('pt-BR'));
                const chartData = sortedDays.map(date => dataByDay[date]);

                const commonOptions = this._getCommonChartOptions();
                this._createOrUpdateChart('graficoAcumuloPluviosidade', {
                    type: 'bar',
                    data: {
                        labels,
                        datasets: [{
                            label: 'Pluviosidade (mm)',
                            data: chartData,
                            backgroundColor: '#1976D2',
                        }]
                    },
                    options: {
                        ...commonOptions,
                        plugins: {
                            ...commonOptions.plugins,
                            legend: { display: false },
                            datalabels: {
                                align: 'end',
                                anchor: 'end',
                                backgroundColor: 'rgba(25, 118, 210, 0.8)',
                                borderRadius: 4,
                                color: 'white',
                                font: {
                                    weight: 'bold'
                                },
                                formatter: (value) => value > 0 ? `${value.toFixed(1)} mm` : '',
                                padding: 6
                            }
                        }
                    }
                });
            },

            renderVelocidadeVentoChart(data) {
                const dataByFazenda = data.reduce((acc, item) => {
                    const fazenda = item.fazendaNome || 'N/A';
                    if (!acc[fazenda]) acc[fazenda] = [];
                    acc[fazenda].push(item.vento);
                    return acc;
                }, {});

                const avgByFazenda = Object.entries(dataByFazenda).map(([name, values]) => ({
                    name,
                    avg: values.reduce((a, b) => a + b, 0) / values.length
                })).sort((a, b) => a.avg - b.avg);


                const labels = avgByFazenda.map(item => item.name);
                const chartData = avgByFazenda.map(item => item.avg);

                const commonOptions = this._getCommonChartOptions({ indexAxis: 'y', hasLongLabels: true });
                this._createOrUpdateChart('graficoMediaVentoFazenda', {
                    type: 'bar',
                    data: {
                        labels,
                        datasets: [{
                            label: 'Velocidade Média do Vento (km/h)',
                            data: chartData,
                            backgroundColor: '#388E3C',
                        }]
                    },
                    options: {
                        ...commonOptions, plugins: {
                            ...commonOptions.plugins,
                            legend: { display: false },
                            datalabels: {
                                align: 'end',
                                anchor: 'end',
                                backgroundColor: 'rgba(56, 142, 60, 0.8)',
                                borderRadius: 4,
                                color: 'white',
                                font: {
                                    weight: 'bold'
                                },
                                formatter: (value) => `${value.toFixed(1)} km/h`,
                                padding: 6
                            }
                        }
                    }
                });
            },

            renderIndiceClimatologicoChart(data) {
                const avgTemp = data.length > 0 ? data.reduce((sum, item) => sum + (item.tempMax + item.tempMin) / 2, 0) / data.length : 0;
                const avgUmidade = data.length > 0 ? data.reduce((sum, item) => sum + item.umidade, 0) / data.length : 0;
                const avgVento = data.length > 0 ? data.reduce((sum, item) => sum + item.vento, 0) / data.length : 0;

                // Normalize data for radar chart (0-100 scale)
                const normalizedTemp = (avgTemp / 50) * 100; // Assuming max temp is 50
                const normalizedUmidade = avgUmidade;
                const normalizedVento = (avgVento / 60) * 100; // Assuming max wind is 60km/h

                const commonOptions = this._getCommonChartOptions();

                this._createOrUpdateChart('graficoIndiceClimatologico', {
                    type: 'radar',
                    data: {
                        labels: ['Temperatura', 'Umidade', 'Vento'],
                        datasets: [{
                            label: 'Índice Climatológico (Normalizado)',
                            data: [normalizedTemp, normalizedUmidade, normalizedVento],
                            fill: true,
                            backgroundColor: 'rgba(245, 124, 0, 0.2)',
                            borderColor: '#F57C00',
                            pointBackgroundColor: '#F57C00',
                        }]
                    },
                    options: {
                        ...commonOptions,
                        scales: {
                            r: {
                                beginAtZero: true,
                                max: 100,
                                grid: { color: commonOptions.scales.y.grid.color },
                                angleLines: { color: commonOptions.scales.y.grid.color },
                                pointLabels: { color: commonOptions.scales.y.ticks.color, font: { size: 14 } },
                                ticks: {
                                    display: false,
                                    stepSize: 20
                                }
                            }
                        },
                        plugins: {
                            ...commonOptions.plugins,
                            legend: { display: false },
                            datalabels: {
                                backgroundColor: 'rgba(245, 124, 0, 0.8)',
                                borderRadius: 4,
                                color: 'white',
                                font: {
                                    weight: 'bold'
                                },
                                formatter: (value, context) => {
                                    if (context.dataIndex === 0) return `${avgTemp.toFixed(1)}°C`;
                                    if (context.dataIndex === 1) return `${avgUmidade.toFixed(1)}%`;
                                    if (context.dataIndex === 2) return `${avgVento.toFixed(1)} km/h`;
                                    return '';
                                },
                                padding: 6
                            }
                        }
                    }
                });
            }

        },

        reports: {
                _fetchAndDownloadReport(endpoint, filters, filename) {
                    const cleanFilters = Object.fromEntries(Object.entries(filters).filter(([_, v]) => v != null && v !== ''));
                    cleanFilters.generatedBy = App.state.currentUser?.username || 'Usuário Desconhecido';
                    if (App.state.currentUser && App.state.currentUser.companyId) {
                        cleanFilters.companyId = App.state.currentUser.companyId;
                    }

                    // Security check: Abort if companyId is missing, preventing cross-tenant data leakage.
                    if (!cleanFilters.companyId) {
                        App.ui.showAlert("Erro de segurança: ID da empresa não especificado. Não é possível gerar o relatório.", "error");
                        console.error("Aborted report generation due to missing companyId.");
                        return;
                    }

                    const params = new URLSearchParams(cleanFilters);
                    const apiUrl = `${App.config.backendUrl}/reports/${endpoint}?${params.toString()}&_cacheBust=${Date.now()}`;

                    App.ui.setLoading(true, "A gerar relatório no servidor...");

                    fetch(apiUrl)
                        .then(response => {
                            if (!response.ok) {
                                return response.text().then(text => { throw new Error(text || `Erro do servidor: ${response.statusText}`) });
                            }
                            return response.blob();
                        })
                        .then(blob => {
                            const url = window.URL.createObjectURL(blob);
                            const a = document.createElement('a');
                            a.style.display = 'none';
                            a.href = url;
                            a.download = filename;
                            document.body.appendChild(a);
                            a.click();
                            window.URL.revokeObjectURL(url);
                            a.remove();
                            App.ui.showAlert('Relatório gerado com sucesso!');
                        })
                        .catch(error => {
                            console.error('Erro ao gerar relatório via API:', error);
                            App.ui.showAlert(`Não foi possível gerar o relatório: ${error.message}`, "error");
                        })
                        .finally(() => {
                            App.ui.setLoading(false);
                        });
                },

                generateBrocamentoPDF() {
                    const { filtroInicio, filtroFim, filtroFazenda, tipoRelatorio, farmTypeFilter } = App.elements.broca;
                    if (!filtroInicio.value || !filtroFim.value) { App.ui.showAlert("Selecione Data Início e Fim.", "warning"); return; }
                    const farmId = filtroFazenda.value;
                    const farm = App.state.fazendas.find(f => f.id === farmId);
                    const selectedTypes = Array.from(farmTypeFilter).filter(cb => cb.checked).map(cb => cb.value);
                    const filters = {
                        inicio: filtroInicio.value,
                        fim: filtroFim.value,
                        fazendaCodigo: farm ? farm.code : '',
                        tipoRelatorio: tipoRelatorio.value,
                        tipos: selectedTypes.join(',')
                    };
                    this._fetchAndDownloadReport('brocamento/pdf', filters, 'relatorio_brocamento.pdf');
                },

                generateBrocamentoCSV() {
                    const { filtroInicio, filtroFim, filtroFazenda, tipoRelatorio, farmTypeFilter } = App.elements.broca;
                    if (!filtroInicio.value || !filtroFim.value) { App.ui.showAlert("Selecione Data Início e Fim.", "warning"); return; }
                    const farmId = filtroFazenda.value;
                    const farm = App.state.fazendas.find(f => f.id === farmId);
                    const selectedTypes = Array.from(farmTypeFilter).filter(cb => cb.checked).map(cb => cb.value);
                    const filters = {
                        inicio: filtroInicio.value,
                        fim: filtroFim.value,
                        fazendaCodigo: farm ? farm.code : '',
                        tipoRelatorio: tipoRelatorio.value,
                        tipos: selectedTypes.join(',')
                    };
                    this._fetchAndDownloadReport('brocamento/csv', filters, 'relatorio_brocamento.csv');
                },

                generatePerdaPDF() {
                    const { filtroInicio, filtroFim, filtroFazenda, filtroTalhao, filtroOperador, filtroFrente, tipoRelatorio, farmTypeFilter } = App.elements.perda;
                    if (!filtroInicio.value || !filtroFim.value) { App.ui.showAlert("Selecione Data Início e Fim.", "warning"); return; }
                    const farmId = filtroFazenda.value;
                    const farm = App.state.fazendas.find(f => f.id === farmId);
                    const selectedTypes = Array.from(farmTypeFilter).filter(cb => cb.checked).map(cb => cb.value);
                    const filters = {
                        inicio: filtroInicio.value,
                        fim: filtroFim.value,
                        fazendaCodigo: farm ? farm.code : '',
                        talhao: filtroTalhao.value,
                        matricula: filtroOperador.value,
                        frenteServico: filtroFrente.value,
                        tipoRelatorio: tipoRelatorio.value,
                        tipos: selectedTypes.join(',')
                    };
                    this._fetchAndDownloadReport('perda/pdf', filters, 'relatorio_perda.pdf');
                },

                generatePerdaCSV() {
                    const { filtroInicio, filtroFim, filtroFazenda, filtroTalhao, filtroOperador, filtroFrente, tipoRelatorio, farmTypeFilter } = App.elements.perda;
                    if (!filtroInicio.value || !filtroFim.value) { App.ui.showAlert("Selecione Data Início e Fim.", "warning"); return; }
                    const farmId = filtroFazenda.value;
                    const farm = App.state.fazendas.find(f => f.id === farmId);
                    const selectedTypes = Array.from(farmTypeFilter).filter(cb => cb.checked).map(cb => cb.value);
                    const filters = {
                        inicio: filtroInicio.value,
                        fim: filtroFim.value,
                        fazendaCodigo: farm ? farm.code : '',
                        talhao: filtroTalhao.value,
                        matricula: filtroOperador.value,
                        frenteServico: filtroFrente.value,
                        tipoRelatorio: tipoRelatorio.value,
                        tipos: selectedTypes.join(',')
                    };
                    this._fetchAndDownloadReport('perda/csv', filters, 'relatorio_perda.csv');
                },

                generateCigarrinhaPDF() {
                    const { filtroInicio, filtroFim, filtroFazenda } = App.elements.cigarrinha;
                    if (!filtroInicio.value || !filtroFim.value) {
                        App.ui.showAlert("Selecione Data Início e Fim.", "warning");
                        return;
                    }
                    const farmId = filtroFazenda.value;
                    const farm = App.state.fazendas.find(f => f.id === farmId);
                    const filters = {
                        inicio: filtroInicio.value,
                        fim: filtroFim.value,
                        fazendaCodigo: farm ? farm.code : ''
                    };
                    this._fetchAndDownloadReport('cigarrinha/pdf', filters, 'relatorio_cigarrinha.pdf');
                },

                generateCigarrinhaCSV() {
                    const { filtroInicio, filtroFim, filtroFazenda } = App.elements.cigarrinha;
                    if (!filtroInicio.value || !filtroFim.value) {
                        App.ui.showAlert("Selecione Data Início e Fim.", "warning");
                        return;
                    }
                    const farmId = filtroFazenda.value;
                    const farm = App.state.fazendas.find(f => f.id === farmId);
                    const filters = {
                        inicio: filtroInicio.value,
                        fim: filtroFim.value,
                        fazendaCodigo: farm ? farm.code : ''
                    };
                    this._fetchAndDownloadReport('cigarrinha/csv', filters, 'relatorio_cigarrinha.csv');
                },

                generateCigarrinhaAmostragemPDF() {
                    const { filtroInicio, filtroFim, filtroFazenda, tipoRelatorio } = App.elements.cigarrinhaAmostragem;
                    if (!filtroInicio.value || !filtroFim.value) {
                        App.ui.showAlert("Selecione Data Início e Fim.", "warning");
                        return;
                    }
                    const farmId = filtroFazenda.value;
                    const farm = App.state.fazendas.find(f => f.id === farmId);
                    const filters = {
                        inicio: filtroInicio.value,
                        fim: filtroFim.value,
                        fazendaCodigo: farm ? farm.code : '',
                        tipoRelatorio: document.getElementById('tipoRelatorioCigarrinhaAmostragem').value
                    };
                    this._fetchAndDownloadReport('cigarrinha-amostragem/pdf', filters, 'relatorio_cigarrinha_amostragem.pdf');
                },

                generateCigarrinhaAmostragemCSV() {
                    const { filtroInicio, filtroFim, filtroFazenda, tipoRelatorio } = App.elements.cigarrinhaAmostragem;
                    if (!filtroInicio.value || !filtroFim.value) {
                        App.ui.showAlert("Selecione Data Início e Fim.", "warning");
                        return;
                    }
                    const farmId = filtroFazenda.value;
                    const farm = App.state.fazendas.find(f => f.id === farmId);
                    const filters = {
                        inicio: filtroInicio.value,
                        fim: filtroFim.value,
                        fazendaCodigo: farm ? farm.code : '',
                        tipoRelatorio: document.getElementById('tipoRelatorioCigarrinhaAmostragem').value
                    };
                    this._fetchAndDownloadReport('cigarrinha-amostragem/csv', filters, 'relatorio_cigarrinha_amostragem.csv');
                },

                generateCustomHarvestReport(format) {
                const { select, optionsContainer, tipoRelatorioSelect } = App.elements.relatorioColheita;
                const planId = select.value;
                const reportType = tipoRelatorioSelect.value;
                
                if (!planId) {
                    App.ui.showAlert("Por favor, selecione um plano de colheita.", "warning");
                    return;
                }
                
                let endpoint = `colheita/${format}`;
                const filters = { planId };
                
                if (reportType === 'detalhado') {
                    const selectedColumns = {};
                    optionsContainer.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                        selectedColumns[cb.dataset.column] = cb.checked;
                    });
                    filters.selectedColumns = JSON.stringify(selectedColumns);
                } else {
                    endpoint = `colheita/mensal/${format}`;
                }
                
                this._fetchAndDownloadReport(endpoint, filters, `relatorio_colheita_${reportType}.${format}`);
            },

            generateArmadilhaPDF() {
                const { tipoRelatorio, inicio, fim, fazendaFiltro } = App.elements.relatorioMonitoramento;
                if (!inicio.value || !fim.value) { App.ui.showAlert("Selecione Data Início e Fim.", "warning"); return; }
                
                const farmId = fazendaFiltro.value;
                const farm = App.state.fazendas.find(f => f.id === farmId);
                const reportType = tipoRelatorio.value;

                const filters = {
                    inicio: inicio.value,
                    fim: fim.value,
                    fazendaCodigo: farm ? farm.code : ''
                };
                
                if (reportType === 'coletadas') {
                    this._fetchAndDownloadReport('armadilhas/pdf', filters, 'relatorio_armadilhas_coletadas.pdf');
                } else {
                    this._fetchAndDownloadReport('armadilhas-ativas/pdf', filters, 'relatorio_armadilhas_instaladas.pdf');
                }
            },

            generateArmadilhaCSV() {
                const { tipoRelatorio, inicio, fim, fazendaFiltro } = App.elements.relatorioMonitoramento;
                if (!inicio.value || !fim.value) { App.ui.showAlert("Selecione Data Início e Fim.", "warning"); return; }

                const farmId = fazendaFiltro.value;
                const farm = App.state.fazendas.find(f => f.id === farmId);
                const reportType = tipoRelatorio.value;

                const filters = {
                    inicio: inicio.value,
                    fim: fim.value,
                    fazendaCodigo: farm ? farm.code : ''
                };
                
                if (reportType === 'coletadas') {
                    this._fetchAndDownloadReport('armadilhas/csv', filters, 'relatorio_armadilhas_coletadas.csv');
                } else {
                    this._fetchAndDownloadReport('armadilhas-ativas/csv', filters, 'relatorio_armadilhas_instaladas.csv');
                }
            },

            generateMonitoramentoPDF() { // Now generates Trap Report
                const { inicio, fim, fazendaFiltro } = App.elements.relatorioMonitoramento;
                if (!inicio.value || !fim.value) { App.ui.showAlert("Selecione Data Início e Fim.", "warning"); return; }
                
                const farmId = fazendaFiltro.value;
                const farm = App.state.fazendas.find(f => f.id === farmId);

                const filters = {
                    inicio: inicio.value,
                    fim: fim.value,
                    fazendaCodigo: farm ? farm.code : ''
                };
                this._fetchAndDownloadReport('armadilhas/pdf', filters, 'relatorio_armadilhas.pdf');
            },

            generatePlantioFazendaPDF() {
                const { inicio, fim, frente, cultura } = App.elements.relatorioPlantio;
                if (!inicio.value || !fim.value) { App.ui.showAlert("Selecione Data Início e Fim.", "warning"); return; }
                const frenteId = frente.value;
                const culturaValue = cultura.value;
                const selectedTypes = Array.from(document.querySelectorAll('#plantioReportFarmTypeFilter input:checked')).map(cb => cb.value);
                const filters = {
                    inicio: inicio.value,
                    fim: fim.value,
                    frenteId: frenteId,
                    cultura: culturaValue,
                    tipos: selectedTypes.join(','),
                };
                this._fetchAndDownloadReport('plantio/fazenda/pdf', filters, 'relatorio_plantio_fazenda.pdf');
            },

            generatePlantioFazendaExcel() {
                const { inicio, fim, frente, cultura } = App.elements.relatorioPlantio;
                if (!inicio.value || !fim.value) { App.ui.showAlert("Selecione Data Início e Fim.", "warning"); return; }
                const frenteId = frente.value;
                const culturaValue = cultura.value;
                const selectedTypes = Array.from(document.querySelectorAll('#plantioReportFarmTypeFilter input:checked')).map(cb => cb.value);
                const filters = {
                    inicio: inicio.value,
                    fim: fim.value,
                    frenteId: frenteId,
                    cultura: culturaValue,
                    tipos: selectedTypes.join(','),
                };
                this._fetchAndDownloadReport('plantio/fazenda/csv', filters, 'relatorio_plantio_fazenda.csv');
            },

            generatePlantioTalhaoPDF() {
                const { inicio, fim, frente, cultura } = App.elements.relatorioPlantio;
                if (!inicio.value || !fim.value) { App.ui.showAlert("Selecione Data Início e Fim.", "warning"); return; }
                const frenteId = frente.value;
                const culturaValue = cultura.value;
                const selectedTypes = Array.from(document.querySelectorAll('#plantioReportFarmTypeFilter input:checked')).map(cb => cb.value);
                const filters = {
                    inicio: inicio.value,
                    fim: fim.value,
                    frenteId: frenteId,
                    cultura: culturaValue,
                    tipos: selectedTypes.join(','),
                };
                this._fetchAndDownloadReport('plantio/talhao/pdf', filters, 'relatorio_plantio_talhao.pdf');
            },

            generatePlantioTalhaoExcel() {
                const { inicio, fim, frente, cultura } = App.elements.relatorioPlantio;
                if (!inicio.value || !fim.value) { App.ui.showAlert("Selecione Data Início e Fim.", "warning"); return; }
                const frenteId = frente.value;
                const culturaValue = cultura.value;
                const selectedTypes = Array.from(document.querySelectorAll('#plantioReportFarmTypeFilter input:checked')).map(cb => cb.value);
                const filters = {
                    inicio: inicio.value,
                    fim: fim.value,
                    frenteId: frenteId,
                    cultura: culturaValue,
                    tipos: selectedTypes.join(','),
                };
                this._fetchAndDownloadReport('plantio/talhao/csv', filters, 'relatorio_plantio_talhao.csv');
            },

            async generateClimaPDF() {
                const { inicio, fim, fazenda } = App.elements.relatorioClima;
                if (!inicio.value || !fim.value) {
                    App.ui.showAlert("Selecione Data Início e Fim.", "warning");
                    return;
                }
                const farm = App.state.fazendas.find(f => f.id === fazenda.value);
                const filters = {
                    inicio: inicio.value,
                    fim: fim.value,
                    fazendaId: farm ? farm.id : '',
                };

                App.ui.setLoading(true, "A preparar gráficos para o relatório...");

                try {
                    // 1. Get chart instances from the dashboard
                    const chartIds = [
                        'graficoVariacaoTemperatura',
                        'graficoAcumuloPluviosidade',
                        'graficoMediaVentoFazenda',
                        'graficoIndiceClimatologico'
                    ];

                    const chartImages = [];
                    for (const id of chartIds) {
                        const chartInstance = App.state.charts[id];
                        if (chartInstance) {
                            chartImages.push(chartInstance.toBase64Image());
                        } else {
                            console.warn(`Chart with id "${id}" not found. It will be skipped in the PDF.`);
                        }
                    }

                    filters.charts = JSON.stringify(chartImages);

                    // 2. Call the report generation
                    this._fetchAndDownloadReport('clima/pdf', filters, 'relatorio_clima.pdf');

                } catch (error) {
                    console.error("Erro ao capturar imagens dos gráficos:", error);
                    App.ui.showAlert("Não foi possível adicionar os gráficos ao relatório. Gerando relatório apenas com dados.", "error");
                    // Still generate the report without charts if there was an error
                    this._fetchAndDownloadReport('clima/pdf', filters, 'relatorio_clima.pdf');
                } finally {
                    // The loading indicator is handled by _fetchAndDownloadReport
                }
            },

            generateClimaCSV() {
                const { inicio, fim, fazenda } = App.elements.relatorioClima;
                if (!inicio.value || !fim.value) { App.ui.showAlert("Selecione Data Início e Fim.", "warning"); return; }
                const farm = App.state.fazendas.find(f => f.id === fazenda.value);
                const filters = {
                    inicio: inicio.value,
                    fim: fim.value,
                    fazendaId: farm ? farm.id : '',
                };
                this._fetchAndDownloadReport('clima/csv', filters, 'relatorio_clima.csv');
            },

            generateMonitoramentoCSV() { // Now generates Trap Report
                const { inicio, fim, fazendaFiltro } = App.elements.relatorioMonitoramento;
                if (!inicio.value || !fim.value) { App.ui.showAlert("Selecione Data Início e Fim.", "warning"); return; }

                const farmId = fazendaFiltro.value;
                const farm = App.state.fazendas.find(f => f.id === farmId);

                const filters = {
                    inicio: inicio.value,
                    fim: fim.value,
                    fazendaCodigo: farm ? farm.code : ''
                };
                this._fetchAndDownloadReport('armadilhas/csv', filters, 'relatorio_armadilhas.csv');
            },

            generateRiskViewPDF() {
                const { inicio, fim } = App.elements.relatorioRisco;
                if (!inicio.value || !fim.value) {
                    App.ui.showAlert("Selecione Data Início e Fim.", "warning");
                    return;
                }
                const riskOnlyCheckbox = document.getElementById('riskOnlyCheckbox');
                const filters = {
                    inicio: inicio.value,
                    fim: fim.value,
                    riskOnly: riskOnlyCheckbox ? riskOnlyCheckbox.checked : false
                };
                this._fetchAndDownloadReport('risk-view/pdf', filters, 'relatorio_de_risco.pdf');
            },

            generateRiskViewCSV() {
                const { inicio, fim } = App.elements.relatorioRisco;
                if (!inicio.value || !fim.value) {
                    App.ui.showAlert("Selecione Data Início e Fim.", "warning");
                    return;
                }
                const riskOnlyCheckbox = document.getElementById('riskOnlyCheckbox');
                const filters = {
                    inicio: inicio.value,
                    fim: fim.value,
                    riskOnly: riskOnlyCheckbox ? riskOnlyCheckbox.checked : false
                };
                this._fetchAndDownloadReport('risk-view/csv', filters, 'relatorio_de_risco.csv');
            },
        },

        pwa: {
            registerServiceWorker() {
                if ('serviceWorker' in navigator) {
                    window.addEventListener('load', async () => {
                        try {
                            const registration = await navigator.serviceWorker.register('./service-worker.js');
                            console.log('ServiceWorker registration successful with scope: ', registration.scope);

                            // ** NOVO: Lógica de Sincronização Periódica **
                            if (registration && 'periodicSync' in registration) {
                                const status = await navigator.permissions.query({
                                    name: 'periodic-background-sync',
                                });

                                if (status.state === 'granted') {
                                    // Tenta registrar a sincronização periódica
                                    try {
                                        await registration.periodicSync.register('sync-offline-writes', {
                                            minInterval: 60 * 60 * 1000, // 1 hora
                                        });
                                        console.log("Sincronização periódica em segundo plano registrada.");
                                    } catch (error) {
                                        console.error("Falha ao registrar a sincronização periódica:", error);
                                    }
                                } else {
                                    console.log("Permissão para sincronização periódica em segundo plano não concedida.");
                                }
                            }

                        } catch (error) {
                            console.log('ServiceWorker registration failed: ', error);
                        }
                    });

                    window.addEventListener('beforeinstallprompt', (e) => {
                        e.preventDefault();
                        App.state.deferredInstallPrompt = e;
                        App.elements.installAppBtn.style.display = 'flex';
                        console.log(`'beforeinstallprompt' event was fired.`);
                    });
                }
            }
        }
    };

    window.addEventListener('offline', () => {
        App.ui.showAlert("Conexão perdida. A operar em modo offline.", "warning");
        if (App.state.connectionCheckInterval) {
            clearInterval(App.state.connectionCheckInterval);
            App.state.connectionCheckInterval = null;
            console.log("Periodic connection check stopped due to offline event.");
        }
    });

    window.addEventListener('online', () => {
        console.log("Browser reports 'online'. Starting active connection checks.");
        App.ui.showSystemNotification("Conexão", "Rede detetada. A verificar acesso à internet...", "info");
        // Clear any previous interval just in case
        if (App.state.connectionCheckInterval) {
            clearInterval(App.state.connectionCheckInterval);
        }
        // Check immediately, then start checking periodically in case the first check fails.
        App.actions.checkActiveConnection();
        App.state.connectionCheckInterval = setInterval(() => App.actions.checkActiveConnection(), 15000); // Check every 15 seconds
    });

    // Inicia a aplicação
    App.init();
    window.App = App; // Expor para testes e depuração
});


