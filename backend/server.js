// server.js - Backend com Geração de PDF e Upload de Shapefile

const express = require('express');
const admin = require('firebase-admin');
const cors = require('cors');
const PDFDocument = require('pdfkit');
const { createObjectCsvWriter } = require('csv-writer');
const path = require('path');
const os = require('os');
const axios = require('axios');
const shp = require('shpjs');
const pointInPolygon = require('point-in-polygon');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const csv = require('csv-parser');
const { Readable } = require('stream');
const xlsx = require('xlsx');

const app = express();
const port = process.env.PORT || 3001;

const corsOptions = {
    origin: 'https://agrovetor.store',
    optionsSuccessStatus: 200
};

app.use(cors(corsOptions));
app.use(express.json({ limit: '50mb' }));

try {
    if (!process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
        throw new Error('A variável de ambiente FIREBASE_SERVICE_ACCOUNT_JSON não está definida.');
    }
    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);
 
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        storageBucket: "agrovetor-v2.firebasestorage.app" // Certifique-se que este é o nome correto do seu bucket
    });

    const db = admin.firestore();
    const bucket = admin.storage().bucket();
    console.log('Firebase Admin SDK inicializado com sucesso e conectado ao bucket.');

    // --- INICIALIZAÇÃO DA IA (GEMINI) ---
    const geminiApiKey = ""; // Chave de API removida a pedido do utilizador.
    let model;
    if (!geminiApiKey) {
        console.warn("A funcionalidade de IA está desativada. Nenhuma chave de API foi fornecida.");
    } else {
        const genAI = new GoogleGenerativeAI(geminiApiKey);
        model = genAI.getGenerativeModel({ model: "gemini-1.5-flash"});
        console.log('Gemini AI Model inicializado com sucesso.');
    }

    app.get('/', (req, res) => {
        res.status(200).send('Servidor de relatórios AgroVetor está online e conectado ao Firebase!');
    });

    // ROTA PARA UPLOAD DO LOGO
    app.post('/upload-logo', async (req, res) => {
        const { logoBase64, companyId } = req.body;
        if (!logoBase64) {
            return res.status(400).send({ message: 'Nenhum dado de imagem Base64 enviado.' });
        }
        if (!companyId) {
            return res.status(400).send({ message: 'O ID da empresa é obrigatório.' });
        }
        try {
            await db.collection('config').doc(companyId).set({ logoBase64: logoBase64 }, { merge: true });
            res.status(200).send({ message: 'Logo carregado com sucesso!' });
        } catch (error) {
            console.error("Erro ao salvar logo Base64 no Firestore:", error);
            res.status(500).send({ message: `Erro no servidor ao carregar logo: ${error.message}` });
        }
    });
 
    // ROTA PARA UPLOAD DO SHAPEFILE
    app.post('/upload-shapefile', async (req, res) => {
        const { fileBase64, companyId } = req.body;
        if (!fileBase64) {
            return res.status(400).send({ message: 'Nenhum dado de arquivo Base64 foi enviado.' });
        }
        if (!companyId) {
            return res.status(400).send({ message: 'O ID da empresa é obrigatório.' });
        }

        try {
            const buffer = Buffer.from(fileBase64, 'base64');
            const filePath = `shapefiles/${companyId}/talhoes.zip`;
            const file = bucket.file(filePath);

            await file.save(buffer, {
                metadata: {
                    contentType: 'application/zip',
                },
            });
            
            await file.makePublic();
            const downloadURL = file.publicUrl();

            await db.collection('config').doc(companyId).set({
                shapefileURL: downloadURL,
                lastUpdated: new Date()
            }, { merge: true });

            res.status(200).send({ message: 'Shapefile enviado com sucesso!', url: downloadURL });

        } catch (error) {
            console.error("Erro no servidor ao fazer upload do shapefile:", error);
            res.status(500).send({ message: `Erro no servidor ao processar o arquivo: ${error.message}` });
        }
    });

    // ROTA PARA INGESTÃO DE RELATÓRIO HISTÓRICO (SEM IA)
    app.post('/api/upload/historical-report', async (req, res) => {
        const { reportData: originalReportData, companyId } = req.body;
        if (!originalReportData) {
            return res.status(400).json({ message: 'Nenhum dado de relatório foi enviado.' });
        }
        if (!companyId) {
            return res.status(400).json({ message: 'O ID da empresa é obrigatório.' });
        }

        try {
            let reportText;

            // Checa se o dado enviado é uma data URL (padrão do FileReader.readAsDataURL)
            if (originalReportData.startsWith('data:')) {
                const base64Data = originalReportData.split(';base64,')[1] || '';
                const buffer = Buffer.from(base64Data, 'base64');

                // Magic number check for ZIP files (XLSX, etc.)
                if (buffer && buffer.length > 1 && buffer[0] === 0x50 && buffer[1] === 0x4B) {
                    const workbook = xlsx.read(buffer, { type: 'buffer' });
                    const sheetName = workbook.SheetNames[0];
                    const worksheet = workbook.Sheets[sheetName];
                    const dataAsJson = xlsx.utils.sheet_to_json(worksheet, { header: 1, defval: "" });
                    reportText = dataAsJson.map(row => row.join(';')).join('\n');
                } else {
                    // Assume que é um arquivo de texto (csv, txt)
                    reportText = buffer.toString('utf8');
                }
            } else {
                reportText = originalReportData;
            }

            const records = [];
            const stream = Readable.from(reportText);

            stream.pipe(csv({
                separator: ';',
                mapHeaders: ({ header }) => header.trim().toLowerCase() // Normaliza o cabeçalho
            }))
            .on('data', (data) => records.push(data))
            .on('end', async () => {
                if (records.length === 0) {
                    return res.status(400).json({ message: "O relatório parece estar vazio ou em um formato incorreto." });
                }

                // Valida se os cabeçalhos necessários existem no primeiro registro
                const requiredHeaders = ['codigofazenda', 'toneladas', 'atr'];
                const firstRecordHeaders = Object.keys(records[0]);
                const missingHeaders = requiredHeaders.filter(h => !firstRecordHeaders.includes(h));

                if (missingHeaders.length > 0) {
                    return res.status(400).json({ message: `Cabeçalhos em falta no seu relatório. É necessário ter as colunas: ${missingHeaders.join(', ')}` });
                }

                const batchSize = 400;
                for (let i = 0; i < records.length; i += batchSize) {
                    const batch = db.batch();
                    const chunk = records.slice(i, i + batchSize);

                    chunk.forEach(record => {
                        const finalRecord = {
                            codigoFazenda: String(record.codigofazenda || '').trim(),
                            toneladas: parseFloat(String(record.toneladas || '0').replace(',', '.')) || 0,
                            atrRealizado: parseFloat(String(record.atr || '0').replace(',', '.')) || 0,
                            importedAt: new Date(),
                            companyId: companyId // Adiciona o ID da empresa ao registo
                        };

                        // Não salva mais campos opcionais como talhao, safra, variedade
                        if (finalRecord.codigoFazenda && finalRecord.toneladas > 0 && finalRecord.atrRealizado > 0) {
                             const docRef = db.collection('historicalHarvests').doc();
                             batch.set(docRef, finalRecord);
                        }
                    });
                    await batch.commit();
                }
                res.status(200).json({ message: `${records.length} registros históricos importados com sucesso!` });
            });

        } catch (error) {
            console.error("Erro na ingestão de relatório histórico:", error);
            res.status(500).json({ message: 'Erro no servidor ao processar o relatório.' });
        }
    });

    // --- ROTA DE GERAÇÃO DA IA (GEMINI) ---
    app.post('/api/gemini/generate', async (req, res) => {
        if (!model) {
            return res.status(503).json({ message: "Esta funcionalidade de IA está temporariamente desativada." });
        }
        const { prompt } = req.body;

        if (!prompt) {
            return res.status(400).json({ message: 'O prompt é obrigatório.' });
        }

        try {
            const result = await model.generateContent(prompt);
            const response = await result.response;
            let text = response.text();

            text = text.replace(/```json/g, '').replace(/```/g, '').trim();
            const jsonResponse = JSON.parse(text);
            res.status(200).json(jsonResponse);

        } catch (error) {
            console.error("Erro ao chamar a API do Gemini:", error);
            res.status(500).json({ message: 'Erro ao comunicar com a IA.' });
        }
    });

    // ROTA PARA CÁLCULO DE ATR PONDERADO
    app.post('/api/calculate-atr', async (req, res) => {
        const { codigoFazenda, companyId } = req.body;
        if (!codigoFazenda) {
            return res.status(400).json({ message: 'O código da fazenda é obrigatório.' });
        }
        if (!companyId) {
            return res.status(400).json({ message: 'O ID da empresa é obrigatório.' });
        }

        try {
            const farmCodeStr = String(codigoFazenda || '').trim();
            const historyQuery = await db.collection('historicalHarvests')
                .where('companyId', '==', companyId)
                .where('codigoFazenda', '==', farmCodeStr)
                .get();

            if (historyQuery.empty) {
                return res.status(200).json({ predicted_atr: 0, message: "Sem histórico para esta fazenda." });
            }

            const historicalData = [];
            historyQuery.forEach(doc => historicalData.push(doc.data()));

            const { totalAtrPonderado, totalToneladas } = historicalData.reduce((acc, data) => {
                const atr = parseFloat(String(data.atrRealizado).replace(',', '.')) || 0;
                const toneladas = parseFloat(String(data.toneladas).replace(',', '.')) || 0;

                if (atr > 0 && toneladas > 0) {
                    acc.totalAtrPonderado += atr * toneladas;
                    acc.totalToneladas += toneladas;
                }
                return acc;
            }, { totalAtrPonderado: 0, totalToneladas: 0 });

            const predicted_atr = totalToneladas > 0 ? totalAtrPonderado / totalToneladas : 0;

            return res.status(200).json({ predicted_atr });

        } catch (e) {
            console.error("Erro ao calcular ATR ponderado no backend:", e);
            res.status(500).json({ message: 'Erro no servidor ao calcular o ATR.' });
        }
    });

    async function deleteCollection(db, collectionPath, batchSize) {
        const collectionRef = db.collection(collectionPath);
        const query = collectionRef.orderBy('__name__').limit(batchSize);

        return new Promise((resolve, reject) => {
            deleteQueryBatch(db, query, resolve, reject);
        });
    }

    async function deleteQueryBatch(db, query, resolve, reject) {
        try {
            const snapshot = await query.get();

            const batchSize = snapshot.size;
            if (batchSize === 0) {
                resolve();
                return;
            }

            const batch = db.batch();
            snapshot.docs.forEach((doc) => {
                batch.delete(doc.ref);
            });
            await batch.commit();

            process.nextTick(() => {
                deleteQueryBatch(db, query, resolve, reject);
            });
        } catch(err) {
            reject(err);
        }
    }

    app.post('/api/delete/historical-data', async (req, res) => {
        const { companyId } = req.body;
        if (!companyId) {
            return res.status(400).json({ message: 'O ID da empresa é obrigatório.' });
        }
        try {
            console.log(`Iniciando a exclusão do histórico para a empresa: ${companyId}`);
            const collectionRef = db.collection('historicalHarvests');
            const query = collectionRef.where('companyId', '==', companyId).limit(400);

            await new Promise((resolve, reject) => {
                deleteQueryBatch(db, query, resolve, reject);
            });

            console.log(`Histórico da empresa ${companyId} excluído com sucesso.`);
            res.status(200).json({ message: 'Todos os dados do histórico da IA para esta empresa foram excluídos com sucesso.' });
        } catch (error) {
            console.error(`Erro ao excluir o histórico da IA para a empresa ${companyId}:`, error);
            res.status(500).json({ message: 'Ocorreu um erro no servidor ao tentar excluir o histórico.' });
        }
    });

    app.post('/api/track', async (req, res) => {
        const { userId, latitude, longitude, companyId } = req.body;

        if (!userId || latitude === undefined || longitude === undefined) {
            return res.status(400).json({ message: 'userId, latitude e longitude são obrigatórios.' });
        }
        if (!companyId) {
            return res.status(400).json({ message: 'O ID da empresa é obrigatório para rastreamento.' });
        }

        try {
            await db.collection('locationHistory').add({
                userId: userId,
                companyId: companyId, // Adiciona o ID da empresa
                location: new admin.firestore.GeoPoint(parseFloat(latitude), parseFloat(longitude)),
                timestamp: admin.firestore.FieldValue.serverTimestamp()
            });
            res.status(200).send({ message: 'Localização registrada com sucesso.' });
        } catch (error) {
            console.error("Erro ao registrar localização:", error);
            res.status(500).json({ message: 'Erro no servidor ao registrar localização.' });
        }
    });

    app.get('/api/history', async (req, res) => {
        const { userId, startDate, endDate, companyId } = req.query;

        if (!userId || !startDate || !endDate) {
            return res.status(400).json({ message: 'userId, startDate e endDate são obrigatórios.' });
        }
        if (!companyId) {
            return res.status(400).json({ message: 'O ID da empresa é obrigatório.' });
        }

        try {
            const query = db.collection('locationHistory')
                .where('companyId', '==', companyId) // Adiciona filtro de empresa
                .where('userId', '==', userId)
                .where('timestamp', '>=', new Date(startDate + 'T00:00:00Z'))
                .where('timestamp', '<=', new Date(endDate + 'T23:59:59Z'));

            const snapshot = await query.get();

            if (snapshot.empty) {
                return res.status(200).json([]);
            }

            const history = [];
            snapshot.forEach(doc => {
                const data = doc.data();
                history.push({
                    id: doc.id,
                    latitude: data.location.latitude,
                    longitude: data.location.longitude,
                timestamp: data.timestamp.toDate()
                });
            });

            // Ordena os resultados manualmente pelo timestamp
            history.sort((a, b) => a.timestamp - b.timestamp);

            res.status(200).json(history);
        } catch (error) {
            console.error("Erro ao buscar histórico de localização:", error);
            res.status(500).json({ message: 'Erro no servidor ao buscar histórico.' });
        }
    });

    // --- FUNÇÕES AUXILIARES ---

    const formatNumber = (num) => {
        if (typeof num !== 'number' || isNaN(num)) {
            return num;
        }
        return parseFloat(num.toFixed(2)).toLocaleString('pt-BR', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
        });
    };

    const sortByDateAndFazenda = (a, b) => {
        const dateComparison = new Date(a.data) - new Date(b.data);
        if (dateComparison !== 0) {
            return dateComparison;
        }
        // Fallback para o código da fazenda (ordem numérica)
        const codeA = parseInt(a.codigo, 10) || 0;
        const codeB = parseInt(b.codigo, 10) || 0;
        return codeA - codeB;
    };

    const safeToDate = (dateInput) => {
        if (!dateInput) return null;
        // Se for um objeto Timestamp do Firestore, use toDate()
        if (dateInput && typeof dateInput.toDate === 'function') {
            return dateInput.toDate();
        }
        // Se já for um objeto Date do JS
        if (dateInput instanceof Date) {
            return dateInput;
        }
        // Se for uma string ou número, tenta criar uma nova Data
        const date = new Date(dateInput);
        if (!isNaN(date.getTime())) {
            return date;
        }
        return null; // Retorna nulo se não conseguir converter
    };

    const getFilteredData = async (collectionName, filters) => {
        // Validação de Segurança: Garante que o companyId foi fornecido.
        if (!filters.companyId) {
            console.error("Tentativa de acesso a getFilteredData sem companyId.");
            return []; // Retorna vazio para evitar qualquer vazamento de dados.
        }

        // A consulta agora busca APENAS os dados da empresa especificada.
        let query = db.collection(collectionName).where('companyId', '==', filters.companyId);

        const snapshot = await query.get();
        let data = [];
        snapshot.forEach(doc => {
            data.push({ id: doc.id, ...doc.data() });
        });
        if (filters.inicio) {
            data = data.filter(d => d.data >= filters.inicio);
        }
        if (filters.fim) {
            data = data.filter(d => d.data <= filters.fim);
        }

        let farmCodesToFilter = null;

        if (filters.fazendaCodigo && filters.fazendaCodigo !== '') {
            farmCodesToFilter = [filters.fazendaCodigo];
        } else if (filters.tipos) {
            const selectedTypes = filters.tipos.split(',').filter(t => t);
            if (selectedTypes.length > 0) {
                // Para fazendas, também precisamos considerar as antigas sem companyId
                const companyFarmsQuery = db.collection('fazendas').where('companyId', '==', filters.companyId);
                const legacyFarmsQuery = db.collection('fazendas').where('companyId', '==', null);
                
                const [companyFarmsSnapshot, legacyFarmsSnapshot] = await Promise.all([
                    companyFarmsQuery.get(),
                    legacyFarmsQuery.get()
                ]);

                let allFarms = [];
                companyFarmsSnapshot.forEach(doc => allFarms.push(doc.data()));
                legacyFarmsSnapshot.forEach(doc => allFarms.push(doc.data()));

                const matchingFarmCodes = allFarms
                    .filter(farm => farm.types && farm.types.some(t => selectedTypes.includes(t)))
                    .map(farm => farm.code);

                if (matchingFarmCodes.length > 0) {
                    farmCodesToFilter = matchingFarmCodes;
                } else {
                    return []; // Se o filtro de tipo não retornar nenhuma fazenda, não há dados a serem mostrados.
                }
            }
        }

        let filteredData = data;

        if (farmCodesToFilter) {
            filteredData = filteredData.filter(d => farmCodesToFilter.includes(d.codigo));
        }
        
        if (filters.matricula) {
            filteredData = filteredData.filter(d => d.matricula === filters.matricula);
        }
        if (filters.talhao) {
            filteredData = filteredData.filter(d => d.talhao && d.talhao.toLowerCase().includes(filters.talhao.toLowerCase()));
        }
        if (filters.frenteServico) {
            filteredData = filteredData.filter(d => d.frenteServico && d.frenteServico.toLowerCase().includes(filters.frenteServico.toLowerCase()));
        }
        
        return filteredData.sort(sortByDateAndFazenda);
    };

    const generatePdfHeader = async (doc, title, companyId) => {
        try {
            let logoBase64 = null;
            // 1. Tenta carregar o logo da empresa específica.
            if (companyId) {
                const configDoc = await db.collection('config').doc(companyId).get();
                if (configDoc.exists && configDoc.data().logoBase64) {
                    logoBase64 = configDoc.data().logoBase64;
                }
            }

            // 2. Se não houver logo específico, busca o da empresa mais antiga (principal).
            if (!logoBase64) {
                const oldestCompanyQuery = await db.collection('companies').orderBy('createdAt', 'asc').limit(1).get();
                if (!oldestCompanyQuery.empty) {
                    const oldestCompanyId = oldestCompanyQuery.docs[0].id;
                    const defaultConfigDoc = await db.collection('config').doc(oldestCompanyId).get();
                    if (defaultConfigDoc.exists && defaultConfigDoc.data().logoBase64) {
                        logoBase64 = defaultConfigDoc.data().logoBase64;
                    }
                }
            }

            // 3. Se um logo foi encontrado (específico ou padrão), desenha-o.
            if (logoBase64) {
                doc.image(logoBase64, doc.page.margins.left, 15, { width: 40 });
            }

        } catch (error) {
            console.error("Não foi possível carregar o logotipo:", error.message);
        }
        
        doc.fontSize(18).font('Helvetica-Bold').text(title, { align: 'center' });
        doc.moveDown(2);
        return doc.y;
    };

    const generatePdfFooter = (doc, generatedBy = 'N/A') => {
        const pageCount = doc.bufferedPageRange().count;
        for (let i = 0; i < pageCount; i++) {
            doc.switchToPage(i);
            
            const footerY = doc.page.height - doc.page.margins.bottom + 10;
            doc.fontSize(8).font('Helvetica')
               .text(`Gerado por: ${generatedBy} em: ${new Date().toLocaleString('pt-BR')}`,
                     doc.page.margins.left,
                     footerY,
                     { align: 'left', lineBreak: false });
        }
    };

    const drawRow = (doc, rowData, y, isHeader = false, isFooter = false, customWidths, textPadding = 5, rowHeight = 18, columnHeadersConfig = [], isClosed = false) => {
        const startX = doc.page.margins.left;
        const fontSize = 8;
        
        if (isHeader || isFooter) {
            doc.font('Helvetica-Bold').fontSize(fontSize);
            doc.rect(startX, y, doc.page.width - doc.page.margins.left - doc.page.margins.right, rowHeight).fillAndStroke('#E8E8E8', '#E8E8E8');
            doc.fillColor('black');
        } else {
            doc.font('Helvetica').fontSize(fontSize);
            if (isClosed) {
                doc.rect(startX, y, doc.page.width - doc.page.margins.left - doc.page.margins.right, rowHeight).fillAndStroke('#f0f0f0', '#f0f0f0');
                doc.fillColor('#999');
            }
        }
        
        let currentX = startX;
        let maxRowHeight = rowHeight;

        rowData.forEach((cell, i) => {
            let columnId = null;
            if (Array.isArray(columnHeadersConfig) && i < columnHeadersConfig.length && columnHeadersConfig[i]) {
                columnId = columnHeadersConfig[i].id;
            }

            const cellWidth = customWidths[i] - (textPadding * 2);
            const textOptions = { width: cellWidth, align: 'left', continued: false };

            if (['talhoes', 'variedade'].includes(columnId)) {
                textOptions.lineBreak = true;
                textOptions.lineGap = 2;
            } else {
                textOptions.lineBreak = false;
            }
            
            const textHeight = doc.heightOfString(String(cell), textOptions);
            maxRowHeight = Math.max(maxRowHeight, textHeight + textPadding * 2);

            doc.text(String(cell), currentX + textPadding, y + textPadding, textOptions);
            currentX += customWidths[i];
        });
        return y + maxRowHeight;
    };

    const checkPageBreak = async (doc, y, title, neededSpace = 40) => {
        if (y > doc.page.height - doc.page.margins.bottom - neededSpace) {
            doc.addPage();
            return await generatePdfHeader(doc, title);
        }
        return y;
    };

    // --- [NOVO] FUNÇÕES AUXILIARES PARA MONITORAMENTO ---
    let cachedShapefiles = {}; // Alterado para um objeto para cache por empresa
    let lastFetchTimes = {};   // Alterado para um objeto para cache por empresa

    const getShapefileData = async (companyId) => {
        if (!companyId) {
            throw new Error('O ID da empresa é obrigatório para obter dados do shapefile.');
        }
        const now = new Date();
        // Cache em memória por 5 minutos para evitar downloads repetidos por empresa
        if (cachedShapefiles[companyId] && lastFetchTimes[companyId] && (now - lastFetchTimes[companyId] < 5 * 60 * 1000)) {
            return cachedShapefiles[companyId];
        }

        const shapefileDoc = await db.collection('config').doc(companyId).get();
        if (!shapefileDoc.exists || !shapefileDoc.data().shapefileURL) {
            // Não lança um erro, apenas retorna nulo para que o relatório não quebre se o shapefile não existir.
            console.warn(`Shapefile não encontrado para a empresa ${companyId}.`);
            return null;
        }
        const url = shapefileDoc.data().shapefileURL;
        
        const response = await axios({ url, responseType: 'arraybuffer' });
        const geojson = await shp(response.data);
        
        cachedShapefiles[companyId] = geojson;
        lastFetchTimes[companyId] = now;
        return geojson;
    };

    const findTalhaoForTrap = (trap, geojsonData) => {
        const point = [trap.longitude, trap.latitude];
        for (const feature of geojsonData.features) {
            if (feature.geometry) {
                if (feature.geometry.type === 'Polygon') {
                    if (pointInPolygon(point, feature.geometry.coordinates[0])) {
                        return feature.properties;
                    }
                } else if (feature.geometry.type === 'MultiPolygon') {
                    for (const polygon of feature.geometry.coordinates) {
                        if (pointInPolygon(point, polygon[0])) {
                            return feature.properties;
                        }
                    }
                }
            }
        }
        return null; // Retorna null se não encontrar
    };

    const findShapefileProp = (props, keys) => {
        if (!props) return null;
        const propKeys = Object.keys(props);
        for (const key of keys) {
            const matchingPropKey = propKeys.find(pk => pk.toLowerCase() === key.toLowerCase());
            if (matchingPropKey && props[matchingPropKey] !== undefined && props[matchingPropKey] !== null) {
                return props[matchingPropKey];
            }
        }
        return null;
    };

    // --- ROTAS DE RELATÓRIOS ---

    const getPlantioData = async (filters) => {
        if (!filters.companyId) {
            console.error("Attempt to access getPlantioData without companyId.");
            return [];
        }

        let query = db.collection('apontamentosPlantio').where('companyId', '==', filters.companyId);

        if (filters.inicio) {
            query = query.where('date', '>=', filters.inicio);
        }
        if (filters.fim) {
            query = query.where('date', '<=', filters.fim);
        }
        if (filters.frenteId) {
            query = query.where('frenteDePlantioId', '==', filters.frenteId);
        }
        if (filters.cultura) {
            query = query.where('culture', '==', filters.cultura);
        }

        const snapshot = await query.get();
        let data = [];
        snapshot.forEach(doc => {
            data.push({ id: doc.id, ...doc.data() });
        });

        let farmCodesToFilter = null;
        if (filters.tipos) {
            const selectedTypes = filters.tipos.split(',').filter(t => t);
            if (selectedTypes.length > 0) {
                const farmsQuery = db.collection('fazendas').where('companyId', '==', filters.companyId).where('types', 'array-contains-any', selectedTypes);
                const farmsSnapshot = await farmsQuery.get();
                const matchingFarmCodes = [];
                farmsSnapshot.forEach(doc => {
                    matchingFarmCodes.push(doc.data().code);
                });
                if (matchingFarmCodes.length > 0) {
                    farmCodesToFilter = matchingFarmCodes;
                } else {
                    return [];
                }
            }
        }

        if(farmCodesToFilter){
            data = data.filter(d => farmCodesToFilter.includes(d.farmCode));
        }


        return data.sort((a, b) => new Date(a.date) - new Date(b.date));
    };

    app.get('/reports/plantio/fazenda/pdf', async (req, res) => {
        const doc = new PDFDocument({ margin: 30, size: 'A4', layout: 'landscape', bufferPages: true });
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', 'attachment; filename=relatorio_plantio_fazenda.pdf');
        doc.pipe(res);

        try {
            const filters = req.query;
            const data = await getPlantioData(filters);
            const title = 'Relatório de Plantio por Fazenda';

            if (data.length === 0) {
                await generatePdfHeader(doc, title, filters.companyId);
                doc.text('Nenhum dado encontrado para os filtros selecionados.');
                generatePdfFooter(doc, filters.generatedBy);
                doc.end();
                return;
            }

            let currentY = await generatePdfHeader(doc, title, filters.companyId);

            const headers = ['Data', 'Fazenda', 'Prestador', 'Matrícula do Líder', 'Variedade Plantada', 'Talhão', 'Área Plant. (ha)', 'Chuva (mm)', 'Obs'];
            const columnWidths = [60, 200, 100, 80, 100, 60, 60, 60, 100];

            currentY = drawRow(doc, headers, currentY, true, false, columnWidths);

            let totalAreaGeral = 0;
            const dataByFarm = {};

            data.forEach(item => {
                item.records.forEach(record => {
                    if (!dataByFarm[item.farmName]) {
                        dataByFarm[item.farmName] = [];
                    }
                    dataByFarm[item.farmName].push({ ...item, ...record });
                });
            });

            for (const farmName of Object.keys(dataByFarm).sort()) {
                let totalAreaFarm = 0;
                const farmRecords = dataByFarm[farmName];
                farmRecords.sort((a,b) => new Date(a.date) - new Date(b.date));

                for (const record of farmRecords) {
                    currentY = await checkPageBreak(doc, currentY, title);
                    const row = [
                        record.date,
                        `${record.farmCode} - ${record.farmName}`,
                        record.provider,
                        record.leaderId,
                        record.variedade,
                        record.talhao,
                        formatNumber(record.area),
                        record.chuva || '',
                        record.obs || ''
                    ];
                    currentY = drawRow(doc, row, currentY, false, false, columnWidths);
                    totalAreaFarm += record.area;
                }

                currentY = await checkPageBreak(doc, currentY, title);
                const subtotalRow = ['', '', '', '', 'SUB TOTAL', '', formatNumber(totalAreaFarm), '', ''];
                currentY = drawRow(doc, subtotalRow, currentY, false, true, columnWidths);
                currentY += 10;
                totalAreaGeral += totalAreaFarm;
            }

            currentY = await checkPageBreak(doc, currentY, title);
            const totalRow = ['', '', '', '', 'TOTAL GERAL', '', formatNumber(totalAreaGeral), '', ''];
            drawRow(doc, totalRow, currentY, false, true, columnWidths);

            generatePdfFooter(doc, filters.generatedBy);
            doc.end();
        } catch (error) {
            console.error("Erro ao gerar PDF de Plantio por Fazenda:", error);
            if (!res.headersSent) {
                res.status(500).send(`Erro ao gerar relatório: ${error.message}`);
            } else {
                doc.end();
            }
        }
    });

    app.get('/reports/plantio/fazenda/csv', async (req, res) => {
        try {
            const filters = req.query;
            const data = await getPlantioData(filters);
            if (data.length === 0) return res.status(404).send('Nenhum dado encontrado.');

            const filePath = path.join(os.tmpdir(), `plantio_fazenda_${Date.now()}.csv`);
            const csvWriter = createObjectCsvWriter({
                path: filePath,
                header: [
                    { id: 'date', title: 'Data' },
                    { id: 'farmName', title: 'Fazenda' },
                    { id: 'provider', title: 'Prestador' },
                    { id: 'leaderId', title: 'Matrícula do Líder' },
                    { id: 'variedade', title: 'Variedade Plantada' },
                    { id: 'talhao', title: 'Talhão' },
                    { id: 'area', title: 'Área Plant. (ha)' },
                    { id: 'chuva', title: 'Chuva (mm)' },
                    { id: 'obs', title: 'Observações' }
                ]
            });

            const records = [];
            data.forEach(item => {
                item.records.forEach(record => {
                    records.push({ ...item, ...record, farmName: `${item.farmCode} - ${item.farmName}` });
                });
            });

            await csvWriter.writeRecords(records);
            res.download(filePath);
        } catch (error) {
            console.error("Erro ao gerar CSV de Plantio por Fazenda:", error);
            res.status(500).send('Erro ao gerar relatório.');
        }
    });

    const getClimaData = async (filters) => {
        if (!filters.companyId) {
            console.error("Attempt to access getClimaData without companyId.");
            return [];
        }

        let query = db.collection('clima').where('companyId', '==', filters.companyId);

        if (filters.inicio) {
            query = query.where('data', '>=', filters.inicio);
        }
        if (filters.fim) {
            query = query.where('data', '<=', filters.fim);
        }

        const snapshot = await query.get();
        let data = [];
        snapshot.forEach(doc => {
            data.push({ id: doc.id, ...doc.data() });
        });

        if (filters.fazendaId) {
            data = data.filter(d => d.fazendaId === filters.fazendaId);
        }

        return data.sort((a, b) => new Date(a.data) - new Date(b.data));
    };

    app.get('/reports/clima/pdf', async (req, res) => {
        const doc = new PDFDocument({ margin: 30, size: 'A4', layout: 'landscape', bufferPages: true });
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', 'attachment; filename=relatorio_climatologico.pdf');
        doc.pipe(res);

        try {
            const filters = req.query;
            const data = await getClimaData(filters);
            const title = 'Relatório Climatológico';

            if (data.length === 0) {
                await generatePdfHeader(doc, title, filters.companyId);
                doc.text('Nenhum dado encontrado para os filtros selecionados.');
                generatePdfFooter(doc, filters.generatedBy);
                doc.end();
                return;
            }

            let currentY = await generatePdfHeader(doc, title, filters.companyId);

            const headers = ['Data', 'Fazenda', 'Talhão', 'Temp. Máx (°C)', 'Temp. Mín (°C)', 'Umidade (%)', 'Pluviosidade (mm)', 'Vento (km/h)', 'Observações'];
            const columnWidths = [60, 140, 80, 80, 80, 80, 80, 80, 100];

            currentY = drawRow(doc, headers, currentY, true, false, columnWidths);

            let totalPluviosidade = 0;
            let totalTempMax = 0;
            let totalTempMin = 0;
            let totalUmidade = 0;
            let totalVento = 0;
            let count = 0;

            for (const item of data) {
                currentY = await checkPageBreak(doc, currentY, title);
                const row = [
                    item.data,
                    item.fazendaNome,
                    item.talhaoNome,
                    formatNumber(item.tempMax),
                    formatNumber(item.tempMin),
                    formatNumber(item.umidade),
                    formatNumber(item.pluviosidade),
                    formatNumber(item.vento),
                    item.obs || ''
                ];
                currentY = drawRow(doc, row, currentY, false, false, columnWidths);

                totalPluviosidade += item.pluviosidade || 0;
                totalTempMax += item.tempMax || 0;
                totalTempMin += item.tempMin || 0;
                totalUmidade += item.umidade || 0;
                totalVento += item.vento || 0;
                count++;
            }

            currentY = await checkPageBreak(doc, currentY, title);
            const summaryRow = [
                'TOTAIS/MÉDIAS', '', '',
                formatNumber(totalTempMax / count),
                formatNumber(totalTempMin / count),
                formatNumber(totalUmidade / count),
                formatNumber(totalPluviosidade),
                formatNumber(totalVento / count),
                ''
            ];
            drawRow(doc, summaryRow, currentY, false, true, columnWidths);

            // [INÍCIO] LÓGICA PARA ADICIONAR GRÁFICOS AO PDF
            if (filters.charts && filters.charts.length > '[]'.length) { // Check for non-empty array string
                try {
                    const charts = JSON.parse(filters.charts);
                    if (Array.isArray(charts) && charts.length > 0) {

                        // Adiciona uma nova página para o anexo de gráficos
                        doc.addPage({ layout: 'landscape', margin: 30 });
                        let chartY = await generatePdfHeader(doc, 'Anexo - Gráficos Climatológicos', filters.companyId);

                        const chartWidth = 450;
                        const chartHeight = 200; // Altura para cada gráfico
                        const marginX = (doc.page.width - chartWidth) / 2; // Centraliza
                        const spaceBetween = 20;

                        for (let i = 0; i < charts.length; i++) {
                            const chartImage = charts[i];

                            // Adiciona uma nova página a cada 2 gráficos
                            if (i > 0 && i % 2 === 0) {
                                doc.addPage({ layout: 'landscape', margin: 30 });
                                chartY = await generatePdfHeader(doc, 'Anexo - Gráficos Climatológicos', filters.companyId);
                            }

                            const yPos = (i % 2 === 0) ? chartY : chartY + chartHeight + spaceBetween;

                            // Verifica se há espaço, senão cria nova página (segurança)
                            if (yPos + chartHeight > doc.page.height - doc.page.margins.bottom) {
                                doc.addPage({ layout: 'landscape', margin: 30 });
                                chartY = await generatePdfHeader(doc, 'Anexo - Gráficos Climatológicos', filters.companyId);
                                doc.image(chartImage, marginX, chartY, {
                                    fit: [chartWidth, chartHeight],
                                    align: 'center'
                                });
                            } else {
                                doc.image(chartImage, marginX, yPos, {
                                    fit: [chartWidth, chartHeight],
                                    align: 'center'
                                });
                            }
                        }
                    }
                } catch (e) {
                    console.error("Erro ao processar e adicionar imagens de gráficos ao PDF:", e);
                    // A geração do PDF continua mesmo se os gráficos falharem.
                }
            }
            // [FIM] LÓGICA PARA ADICIONAR GRÁFICOS AO PDF


            generatePdfFooter(doc, filters.generatedBy);
            doc.end();
        } catch (error) {
            console.error("Erro ao gerar PDF Climatológico:", error);
            if (!res.headersSent) {
                res.status(500).send(`Erro ao gerar relatório: ${error.message}`);
            } else {
                doc.end();
            }
        }
    });

    app.get('/reports/clima/csv', async (req, res) => {
        try {
            const filters = req.query;
            const data = await getClimaData(filters);
            if (data.length === 0) return res.status(404).send('Nenhum dado encontrado.');

            const filePath = path.join(os.tmpdir(), `clima_${Date.now()}.csv`);
            const csvWriter = createObjectCsvWriter({
                path: filePath,
                header: [
                    { id: 'data', title: 'Data' },
                    { id: 'fazendaNome', title: 'Fazenda' },
                    { id: 'talhaoNome', title: 'Talhão' },
                    { id: 'tempMax', title: 'Temperatura Máxima (°C)' },
                    { id: 'tempMin', title: 'Temperatura Mínima (°C)' },
                    { id: 'umidade', title: 'Umidade Relativa (%)' },
                    { id: 'pluviosidade', title: 'Pluviosidade (mm)' },
                    { id: 'vento', title: 'Velocidade do Vento (km/h)' },
                    { id: 'obs', title: 'Observações' }
                ]
            });

            await csvWriter.writeRecords(data);
            res.download(filePath);
        } catch (error) {
            console.error("Erro ao gerar CSV de Clima:", error);
            res.status(500).send('Erro ao gerar relatório.');
        }
    });

    app.get('/reports/plantio/talhao/pdf', async (req, res) => {
        const doc = new PDFDocument({ margin: 30, size: 'A4', layout: 'landscape', bufferPages: true });
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', 'attachment; filename=relatorio_plantio_talhao.pdf');
        doc.pipe(res);

        try {
            const filters = req.query;
            const data = await getPlantioData(filters);
            const title = 'Relatório de Plantio por Talhão';

            if (data.length === 0) {
                await generatePdfHeader(doc, title, filters.companyId);
                doc.text('Nenhum dado encontrado para os filtros selecionados.');
                generatePdfFooter(doc, filters.generatedBy);
                doc.end();
                return;
            }

            let currentY = await generatePdfHeader(doc, title, filters.companyId);

            const headers = ['Data', 'Fazenda', 'Talhão', 'Variedade Plantada', 'Prestador', 'Área Plant. (ha)', 'Chuva (mm)', 'Obs'];
            const columnWidths = [60, 220, 100, 100, 100, 60, 60, 100];

            currentY = drawRow(doc, headers, currentY, true, false, columnWidths);

            let totalAreaGeral = 0;
            const allRecords = [];
            data.forEach(item => {
                item.records.forEach(record => {
                    allRecords.push({ ...item, ...record });
                });
            });

            allRecords.sort((a, b) => {
                const farmNameA = `${a.farmCode} - ${a.farmName}`;
                const farmNameB = `${b.farmCode} - ${b.farmName}`;
                if (farmNameA < farmNameB) return -1;
                if (farmNameA > farmNameB) return 1;
                return new Date(a.date) - new Date(b.date);
            });

            for (const record of allRecords) {
                    currentY = await checkPageBreak(doc, currentY, title);
                    const row = [
                        record.date,
                        `${record.farmCode} - ${record.farmName}`,
                        record.talhao,
                        record.variedade,
                        record.provider,
                        formatNumber(record.area),
                        record.chuva || '',
                        record.obs || ''
                    ];
                    currentY = drawRow(doc, row, currentY, false, false, columnWidths);
                    totalAreaGeral += record.area;
            }

            currentY = await checkPageBreak(doc, currentY, title);
            const totalRow = ['', '', '', '', '', 'Total Geral', formatNumber(totalAreaGeral), '', ''];
            drawRow(doc, totalRow, currentY, false, true, columnWidths);

            generatePdfFooter(doc, filters.generatedBy);
            doc.end();
        } catch (error) {
            console.error("Erro ao gerar PDF de Plantio por Talhão:", error);
            if (!res.headersSent) {
                res.status(500).send(`Erro ao gerar relatório: ${error.message}`);
            } else {
                doc.end();
            }
        }
    });

    app.get('/reports/plantio/talhao/csv', async (req, res) => {
        try {
            const filters = req.query;
            const data = await getPlantioData(filters);
            if (data.length === 0) return res.status(404).send('Nenhum dado encontrado.');

            const filePath = path.join(os.tmpdir(), `plantio_talhao_${Date.now()}.csv`);
            const csvWriter = createObjectCsvWriter({
                path: filePath,
                header: [
                    { id: 'date', title: 'Data' },
                    { id: 'farmName', title: 'Fazenda' },
                    { id: 'talhao', title: 'Talhão' },
                    { id: 'variedade', title: 'Variedade Plantada' },
                    { id: 'provider', title: 'Prestador' },
                    { id: 'area', title: 'Área Plant. (ha)' },
                    { id: 'chuva', title: 'Chuva (mm)' },
                    { id: 'obs', title: 'Observações' }
                ]
            });

            const records = [];
            data.forEach(item => {
                item.records.forEach(record => {
                    records.push({ ...item, ...record, farmName: `${item.farmCode} - ${item.farmName}` });
                });
            });

            await csvWriter.writeRecords(records);
            res.download(filePath);
        } catch (error) {
            console.error("Erro ao gerar CSV de Plantio por Talhão:", error);
            res.status(500).send('Erro ao gerar relatório.');
        }
    });

    app.get('/reports/brocamento/pdf', async (req, res) => {
        const doc = new PDFDocument({ margin: 30, size: 'A4', layout: 'landscape', bufferPages: true });
        
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', 'attachment; filename=relatorio_brocamento.pdf');
        doc.pipe(res);

        try {
            const filters = req.query;
            const data = await getFilteredData('registros', filters);
            const title = 'Relatório de Inspeção de Broca';

            if (data.length === 0) {
                await generatePdfHeader(doc, title);
                doc.text('Nenhum dado encontrado para os filtros selecionados.');
                generatePdfFooter(doc, filters.generatedBy);
                doc.end();
                return;
            }
            
            const fazendasSnapshot = await db.collection('fazendas').where('companyId', '==', filters.companyId).get();
            const fazendasData = {};
            fazendasSnapshot.forEach(docSnap => {
                fazendasData[docSnap.data().code] = docSnap.data();
            });

            const enrichedData = data.map(reg => {
                const farm = fazendasData[reg.codigo];
                const talhao = farm?.talhoes.find(t => t.name.toUpperCase() === reg.talhao.toUpperCase());
                return { ...reg, variedade: talhao?.variedade || 'N/A' };
            });

            const isModelB = filters.tipoRelatorio === 'B';
            
            let currentY = await generatePdfHeader(doc, title);

            const headersA = ['Fazenda', 'Data', 'Talhão', 'Variedade', 'Corte', 'Entrenós', 'Base', 'Meio', 'Topo', 'Brocado', '% Broca'];
            const columnWidthsA = [160, 60, 60, 100, 80, 60, 45, 45, 45, 55, 62];
            const headersB = ['Data', 'Talhão', 'Variedade', 'Corte', 'Entrenós', 'Base', 'Meio', 'Topo', 'Brocado', '% Broca'];
            const columnWidthsB = [75, 80, 160, 90, 75, 50, 50, 50, 70, 77];

            const headersAConfig = headersA.map(title => ({ id: title.toLowerCase().replace(/[^a-z0-9]/g, ''), title: title }));
            const headersBConfig = headersB.map(title => ({ id: title.toLowerCase().replace(/[^a-z0-9]/g, ''), title: title }));


            if (!isModelB) { // Modelo A
                currentY = drawRow(doc, headersA, currentY, true, false, columnWidthsA, 5, 18, headersAConfig);
                for(const r of enrichedData) {
                    currentY = await checkPageBreak(doc, currentY, title);
                    currentY = drawRow(doc, [`${r.codigo} - ${r.fazenda}`, r.data, r.talhao, r.variedade, r.corte, r.entrenos, r.base, r.meio, r.topo, r.brocado, r.brocamento], currentY, false, false, columnWidthsA, 5, 18, headersAConfig);
                }
            } else { // Modelo B
                const groupedData = enrichedData.reduce((acc, reg) => {
                    const key = `${reg.codigo} - ${reg.fazenda}`;
                    if (!acc[key]) acc[key] = [];
                    acc[key].push(reg);
                    return acc;
                }, {});

                for (const fazendaKey of Object.keys(groupedData).sort()) {
                    currentY = await checkPageBreak(doc, currentY, title, 40);
                    doc.y = currentY;
                    doc.fontSize(12).font('Helvetica-Bold').text(fazendaKey, doc.page.margins.left, currentY, { align: 'left' });
                    currentY = doc.y + 5;

                    currentY = await checkPageBreak(doc, currentY, title);
                    currentY = drawRow(doc, headersB, currentY, true, false, columnWidthsB, 5, 18, headersBConfig);

                    const farmData = groupedData[fazendaKey];
                    for(const r of farmData) {
                        currentY = await checkPageBreak(doc, currentY, title);
                        currentY = drawRow(doc, [r.data, r.talhao, r.variedade, r.corte, r.entrenos, r.base, r.meio, r.topo, r.brocado, r.brocamento], currentY, false, false, columnWidthsB, 5, 18, headersBConfig);
                    }
                    
                    const subTotalEntrenos = farmData.reduce((sum, r) => sum + r.entrenos, 0);
                    const subTotalBrocado = farmData.reduce((sum, r) => sum + r.brocado, 0);
                    const subTotalBase = farmData.reduce((sum, r) => sum + r.base, 0);
                    const subTotalMeio = farmData.reduce((sum, r) => sum + r.meio, 0);
                    const subTotalTopo = farmData.reduce((sum, r) => sum + r.topo, 0);
                    const subTotalPercent = subTotalEntrenos > 0 ? ((subTotalBrocado / subTotalEntrenos) * 100).toFixed(2).replace('.', ',') + '%' : '0,00%';
                    
                    const subtotalRow = ['', '', '', 'Sub Total', subTotalEntrenos, subTotalBase, subTotalMeio, subTotalTopo, subTotalBrocado, subTotalPercent];
                    currentY = drawRow(doc, subtotalRow, currentY, false, true, columnWidthsB, 5, 18, headersBConfig);
                    currentY += 10;
                }
            }
            
            const grandTotalEntrenos = enrichedData.reduce((sum, r) => sum + r.entrenos, 0);
            const grandTotalBrocado = enrichedData.reduce((sum, r) => sum + r.brocado, 0);
            const grandTotalBase = enrichedData.reduce((sum, r) => sum + r.base, 0);
            const grandTotalMeio = enrichedData.reduce((sum, r) => sum + r.meio, 0);
            const grandTotalTopo = enrichedData.reduce((sum, r) => sum + r.topo, 0);
            const totalPercent = grandTotalEntrenos > 0 ? ((grandTotalBrocado / grandTotalEntrenos) * 100).toFixed(2).replace('.', ',') + '%' : '0,00%';

            currentY = await checkPageBreak(doc, currentY, title, 40);
            doc.y = currentY;
            
            if (!isModelB) {
                const totalRowData = ['', '', '', '', 'Total Geral', grandTotalEntrenos, grandTotalBase, grandTotalMeio, grandTotalTopo, grandTotalBrocado, totalPercent];
                drawRow(doc, totalRowData, currentY, false, true, columnWidthsA, 5, 18, headersAConfig);
            } else {
                const totalRowDataB = ['', '', '', 'Total Geral', grandTotalEntrenos, grandTotalBase, grandTotalMeio, grandTotalTopo, grandTotalBrocado, totalPercent];
                drawRow(doc, totalRowDataB, currentY, false, true, columnWidthsB, 5, 18, headersBConfig);
            }

            generatePdfFooter(doc, filters.generatedBy);
            doc.end();
        } catch (error) {
            console.error("Erro ao gerar PDF de Brocamento:", error);
            if (!res.headersSent) {
                res.status(500).send(`Erro ao gerar relatório: ${error.message}`);
            } else {
                doc.end();
            }
        }
    });

    app.get('/reports/brocamento/csv', async (req, res) => {
        try {
            const data = await getFilteredData('registros', req.query);
            if (data.length === 0) return res.status(404).send('Nenhum dado encontrado.');
            
            const filePath = path.join(os.tmpdir(), `brocamento_${Date.now()}.csv`);
            const csvWriter = createObjectCsvWriter({
                path: filePath,
                header: [
                    {id: 'fazenda', title: 'Fazenda'}, {id: 'data', title: 'Data'}, {id: 'talhao', title: 'Talhão'},
                    {id: 'corte', title: 'Corte'}, {id: 'entrenos', title: 'Entrenós'}, {id: 'brocado', title: 'Brocado'},
                    {id: 'brocamento', title: 'Brocamento (%)'}
                ]
            });
            const records = data.map(r => ({ ...r, fazenda: `${r.codigo} - ${r.fazenda}` }));
            await csvWriter.writeRecords(records);
            res.download(filePath);
        } catch (error) { res.status(500).send('Erro ao gerar relatório.'); }
    });

    app.get('/reports/perda/pdf', async (req, res) => {
        const doc = new PDFDocument({ margin: 30, size: 'A4', layout: 'landscape', bufferPages: true });
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename=relatorio_perda.pdf`);
        doc.pipe(res);

        try {
            const filters = req.query;
            const data = await getFilteredData('perdas', filters);
            const isDetailed = filters.tipoRelatorio === 'B';
            const title = isDetailed ? 'Relatório de Perda Detalhado' : 'Relatório de Perda Resumido';

            if (data.length === 0) {
                await generatePdfHeader(doc, title);
                doc.text('Nenhum dado encontrado para os filtros selecionados.');
                generatePdfFooter(doc, filters.generatedBy);
                doc.end();
                return;
            }
            
            let currentY = await generatePdfHeader(doc, title);

            const headersA = ['Data', 'Fazenda', 'Talhão', 'Frente', 'Turno', 'Operador', 'Total'];
            const columnWidthsA = [80, 160, 80, 100, 60, 120, 80];
            const headersB = ['Data', 'Fazenda', 'Talhão', 'Frente', 'Turno', 'Operador', 'C.Inteira', 'Tolete', 'Toco', 'Ponta', 'Estilhaco', 'Pedaco', 'Total'];
            const columnWidthsB = [60, 120, 60, 70, 40, 90, 50, 50, 40, 40, 50, 50, 50];

            const headersAConfig = headersA.map(title => ({ id: title.toLowerCase().replace(/[^a-z0-9]/g, ''), title: title }));
            const headersBConfig = headersB.map(title => ({ id: title.toLowerCase().replace(/[^a-z0-9]/g, ''), title: title }));
            
            const rowHeight = 18;
            const textPadding = 5;

            if (!isDetailed) { // Modelo A - Resumido
                currentY = drawRow(doc, headersA, currentY, true, false, columnWidthsA, textPadding, rowHeight, headersAConfig);
                for(const p of data) {
                    currentY = await checkPageBreak(doc, currentY, title);
                    currentY = drawRow(doc, [p.data, `${p.codigo} - ${p.fazenda}`, p.talhao, p.frenteServico, p.turno, p.operador, formatNumber(p.total)], currentY, false, false, columnWidthsA, textPadding, rowHeight, headersAConfig);
                }
            } else { // Modelo B - Detalhado
                const groupedData = data.reduce((acc, p) => {
                    const key = `${p.codigo} - ${p.fazenda}`;
                    if (!acc[key]) acc[key] = [];
                    acc[key].push(p);
                    return acc;
                }, {});

                for (const fazendaKey of Object.keys(groupedData).sort()) {
                    currentY = await checkPageBreak(doc, currentY, title, 40);
                    doc.y = currentY;
                    doc.fontSize(12).font('Helvetica-Bold').text(fazendaKey, doc.page.margins.left, currentY, { align: 'left' });
                    currentY = doc.y + 5;

                    currentY = await checkPageBreak(doc, currentY, title);
                    currentY = drawRow(doc, headersB, currentY, true, false, columnWidthsB, textPadding, rowHeight, headersBConfig);

                    const farmData = groupedData[fazendaKey];
                    for(const p of farmData) {
                        currentY = await checkPageBreak(doc, currentY, title);
                        currentY = drawRow(doc, [p.data, `${p.codigo} - ${p.fazenda}`, p.talhao, p.frenteServico, p.turno, p.operador, formatNumber(p.canaInteira), formatNumber(p.tolete), formatNumber(p.toco), formatNumber(p.ponta), formatNumber(p.estilhaco), formatNumber(p.pedaco), formatNumber(p.total)], currentY, false, false, columnWidthsB, textPadding, rowHeight, headersBConfig);
                    }
                    
                    const subTotalCanaInteira = farmData.reduce((sum, p) => sum + p.canaInteira, 0);
                    const subTotalTolete = farmData.reduce((sum, p) => sum + p.tolete, 0);
                    const subTotalToco = farmData.reduce((sum, p) => sum + p.toco, 0);
                    const subTotalPonta = farmData.reduce((sum, p) => sum + p.ponta, 0);
                    const subTotalEstilhaco = farmData.reduce((sum, p) => sum + p.estilhaco, 0);
                    const subTotalPedaco = farmData.reduce((sum, p) => sum + p.pedaco, 0);
                    const subTotal = farmData.reduce((sum, p) => sum + p.total, 0);

                    const subtotalRow = ['', '', '', '', '', 'Sub Total', formatNumber(subTotalCanaInteira), formatNumber(subTotalTolete), formatNumber(subTotalToco), formatNumber(subTotalPonta), formatNumber(subTotalEstilhaco), formatNumber(subTotalPedaco), formatNumber(subTotal)];
                    currentY = drawRow(doc, subtotalRow, currentY, false, true, columnWidthsB, textPadding, rowHeight, headersBConfig);
                    currentY += 10;
                }
            }
            
            const grandTotalCanaInteira = data.reduce((sum, p) => sum + p.canaInteira, 0);
            const grandTotalTolete = data.reduce((sum, p) => sum + p.tolete, 0);
            const grandTotalToco = data.reduce((sum, p) => sum + p.toco, 0);
            const grandTotalPonta = data.reduce((sum, p) => sum + p.ponta, 0);
            const grandTotalEstilhaco = data.reduce((sum, p) => sum + p.estilhaco, 0);
            const grandTotalPedaco = data.reduce((sum, p) => sum + p.pedaco, 0);
            const grandTotal = data.reduce((sum, p) => sum + p.total, 0);

            currentY = await checkPageBreak(doc, currentY, title, 40);
            doc.y = currentY;

            if (!isDetailed) {
                const totalRowData = ['', '', '', '', '', 'Total Geral', formatNumber(grandTotal)];
                drawRow(doc, totalRowData, currentY, false, true, columnWidthsA, textPadding, rowHeight, headersAConfig);
            } else {
                const totalRowData = ['', '', '', '', '', 'Total Geral', formatNumber(grandTotalCanaInteira), formatNumber(grandTotalTolete), formatNumber(grandTotalToco), formatNumber(grandTotalPonta), formatNumber(grandTotalEstilhaco), formatNumber(grandTotalPedaco), formatNumber(grandTotal)];
                drawRow(doc, totalRowData, currentY, false, true, columnWidthsB, textPadding, rowHeight, headersBConfig);
            }

            generatePdfFooter(doc, filters.generatedBy);
            doc.end();
        } catch (error) {
            console.error("Erro ao gerar PDF de Perda:", error);
            if (!res.headersSent) {
                res.status(500).send(`Erro ao gerar relatório: ${error.message}`);
            } else {
                doc.end();
            }
        }
    });

    app.get('/reports/perda/csv', async (req, res) => {
        try {
            const filters = req.query;
            const data = await getFilteredData('perdas', filters);
            if (data.length === 0) return res.status(404).send('Nenhum dado encontrado.');

            const isDetailed = filters.tipoRelatorio === 'B';
            const filePath = path.join(os.tmpdir(), `perda_${Date.now()}.csv`);
            let header, records;

            if (isDetailed) {
                header = [
                    {id: 'data', title: 'Data'}, {id: 'fazenda', title: 'Fazenda'}, {id: 'talhao', title: 'Talhão'}, {id: 'frenteServico', title: 'Frente'},
                    {id: 'turno', title: 'Turno'}, {id: 'operador', title: 'Operador'}, {id: 'canaInteira', title: 'C.Inteira'}, {id: 'tolete', title: 'Tolete'},
                    {id: 'toco', title: 'Toco'}, {id: 'ponta', title: 'Ponta'}, {id: 'estilhaco', title: 'Estilhaço'}, {id: 'pedaco', title: 'Pedaço'}, {id: 'total', title: 'Total'}
                ];
                records = data.map(p => ({ ...p, fazenda: `${p.codigo} - ${p.fazenda}` }));
            } else {
                header = [
                    {id: 'data', title: 'Data'}, {id: 'fazenda', title: 'Fazenda'}, {id: 'talhao', title: 'Talhão'}, {id: 'frenteServico', title: 'Frente'},
                    {id: 'turno', title: 'Turno'}, {id: 'operador', title: 'Operador'}, {id: 'total', title: 'Total'}
                ];
                records = data.map(p => ({ data: p.data, fazenda: `${p.codigo} - ${p.fazenda}`, talhao: p.talhao, frenteServico: p.frenteServico, turno: p.turno, operador: p.operador, total: p.total }));
            }
            
            const csvWriter = createObjectCsvWriter({ path: filePath, header });
            await csvWriter.writeRecords(records);
            res.download(filePath);
        } catch (error) { res.status(500).send('Erro ao gerar relatório.'); }
    });

    app.get('/reports/cigarrinha/pdf', async (req, res) => {
        const doc = new PDFDocument({ margin: 30, size: 'A4', layout: 'landscape', bufferPages: true });

        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', 'attachment; filename=relatorio_cigarrinha.pdf');
        doc.pipe(res);

        try {
            const filters = req.query;
            const data = await getFilteredData('cigarrinha', filters);
            const title = 'Relatório de Monitoramento de Cigarrinha';

            if (data.length === 0) {
                await generatePdfHeader(doc, title);
                doc.text('Nenhum dado encontrado para os filtros selecionados.');
                generatePdfFooter(doc, filters.generatedBy);
                doc.end();
                return;
            }

            const fazendasSnapshot = await db.collection('fazendas').where('companyId', '==', filters.companyId).get();
            const fazendasData = {};
            fazendasSnapshot.forEach(docSnap => {
                fazendasData[docSnap.data().code] = docSnap.data();
            });

            const enrichedData = data.map(reg => {
                const farm = fazendasData[reg.codigo];
                const talhao = farm?.talhoes.find(t => t.name.toUpperCase() === reg.talhao.toUpperCase());
                return { ...reg, variedade: talhao?.variedade || 'N/A' };
            });

            let currentY = await generatePdfHeader(doc, title);

            const headers = ['Data', 'Fazenda', 'Talhão', 'Variedade', 'F1', 'F2', 'F3', 'F4', 'F5', 'Adulto', 'Resultado'];
            const columnWidths = [80, 180, 80, 100, 40, 40, 40, 40, 40, 50, 72];
            const headersConfig = headers.map(title => ({ id: title.toLowerCase().replace(/[^a-z0-9]/g, ''), title: title }));

            currentY = drawRow(doc, headers, currentY, true, false, columnWidths, 5, 18, headersConfig);

            for(const r of enrichedData) {
                currentY = await checkPageBreak(doc, currentY, title);
                const date = new Date(r.data + 'T03:00:00Z');
                const formattedDate = date.toLocaleDateString('pt-BR');
                const row = [
                    formattedDate,
                    `${r.codigo} - ${r.fazenda}`,
                    r.talhao,
                    r.variedade,
                    r.fase1,
                    r.fase2,
                    r.fase3,
                    r.fase4,
                    r.fase5,
                    r.adulto ? 'Sim' : 'Não',
                    r.resultado
                ];
                currentY = drawRow(doc, row, currentY, false, false, columnWidths, 5, 18, headersConfig);
            }

            generatePdfFooter(doc, filters.generatedBy);
            doc.end();
        } catch (error) {
            console.error("Erro ao gerar PDF de Cigarrinha:", error);
            if (!res.headersSent) {
                res.status(500).send(`Erro ao gerar relatório: ${error.message}`);
            } else {
                doc.end();
            }
        }
    });

    app.get('/reports/cigarrinha-amostragem/pdf', async (req, res) => {
        const doc = new PDFDocument({ margin: 30, size: 'A4', layout: 'landscape', bufferPages: true });
        const { tipoRelatorio = 'detalhado' } = req.query;
        const filename = `relatorio_cigarrinha_amostragem_${tipoRelatorio}.pdf`;

        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename=${filename}`);
        doc.pipe(res);

        try {
            const filters = req.query;
            const data = await getFilteredData('cigarrinhaAmostragem', filters);
            const title = `Relatório de Cigarrinha (Amostragem) - ${tipoRelatorio.charAt(0).toUpperCase() + tipoRelatorio.slice(1)}`;

            if (data.length === 0) {
                await generatePdfHeader(doc, title);
                doc.text('Nenhum dado encontrado para os filtros selecionados.');
                generatePdfFooter(doc, filters.generatedBy);
                doc.end();
                return;
            }

            let currentY = await generatePdfHeader(doc, title);

            if (tipoRelatorio === 'resumido') {
                const groupedData = data.reduce((acc, r) => {
                    const date = new Date(r.data + 'T03:00:00Z');
                    const formattedDate = date.toLocaleDateString('pt-BR');
                    const key = `${formattedDate}|${r.codigo}|${r.fazenda}|${r.talhao}`;

                    if (!acc[key]) {
                        acc[key] = {
                            data: r.data, // Preserva a data original para ordenação
                            formattedDate: formattedDate, // Usa a data formatada para exibição
                            codigo: r.codigo,
                            fazenda: r.fazenda,
                            talhao: r.talhao,
                            variedade: r.variedade,
                            fase1: 0, fase2: 0, fase3: 0, fase4: 0, fase5: 0,
                        };
                    }
                    r.amostras.forEach(amostra => {
                        acc[key].fase1 += amostra.fase1 || 0;
                        acc[key].fase2 += amostra.fase2 || 0;
                        acc[key].fase3 += amostra.fase3 || 0;
                        acc[key].fase4 += amostra.fase4 || 0;
                        acc[key].fase5 += amostra.fase5 || 0;
                    });
                    return acc;
                }, {});

                const headers = ['Data', 'Fazenda', 'Talhão', 'Variedade', 'Fase 1 (Soma)', 'Fase 2 (Soma)', 'Fase 3 (Soma)', 'Fase 4 (Soma)', 'Fase 5 (Soma)'];
                const columnWidths = [80, 150, 80, 100, 60, 60, 60, 60, 72];
                currentY = drawRow(doc, headers, currentY, true, false, columnWidths);

                const summarizedData = Object.values(groupedData);
                summarizedData.sort(sortByDateAndFazenda);

                for (const group of summarizedData) {
                    const row = [
                        group.formattedDate,
                        `${group.codigo} - ${group.fazenda}`,
                        group.talhao,
                        group.variedade,
                        group.fase1, group.fase2, group.fase3, group.fase4, group.fase5
                    ];
                    currentY = await checkPageBreak(doc, currentY, title);
                    currentY = drawRow(doc, row, currentY, false, false, columnWidths);
                }

            } else if (tipoRelatorio === 'final') {
                const headers = ['Fazenda', 'Data', 'Variedade', 'Adulto', 'Fase1', 'Fase2', 'Fase3', 'Fase4', 'Fase5', 'Resultado Final'];
                const columnWidths = [190, 70, 120, 50, 45, 45, 45, 45, 45, 82];
                currentY = drawRow(doc, headers, currentY, true, false, columnWidths);

                for (const r of data) {
                    const date = new Date(r.data + 'T03:00:00Z');
                    const formattedDate = date.toLocaleDateString('pt-BR');

                    const totalFases = r.amostras.reduce((acc, amostra) => {
                        acc.f1 += amostra.fase1 || 0;
                        acc.f2 += amostra.fase2 || 0;
                        acc.f3 += amostra.fase3 || 0;
                        acc.f4 += amostra.fase4 || 0;
                        acc.f5 += amostra.fase5 || 0;
                        return acc;
                    }, { f1: 0, f2: 0, f3: 0, f4: 0, f5: 0 });

                    const row = [
                        `${r.codigo} - ${r.fazenda}`,
                        formattedDate,
                        r.variedade,
                        r.adulto ? 'Sim' : 'Não',
                        totalFases.f1,
                        totalFases.f2,
                        totalFases.f3,
                        totalFases.f4,
                        totalFases.f5,
                        (r.resultado || 0).toFixed(2).replace('.', ',')
                    ];
                    currentY = await checkPageBreak(doc, currentY, title);
                    currentY = drawRow(doc, row, currentY, false, false, columnWidths);
                }
            } else { // Detalhado
                const headers = ['Fazenda', 'Talhão', 'Data', 'Variedade', 'Adulto', 'Nº Amostra', 'F1', 'F2', 'F3', 'F4', 'F5', 'Resultado Amostra'];
                const columnWidths = [140, 70, 65, 100, 50, 60, 40, 40, 40, 40, 40, 97];
                currentY = drawRow(doc, headers, currentY, true, false, columnWidths);
                const divisor = parseInt(filters.divisor, 10) || parseInt(data[0]?.divisor || '5', 10);

                for(const r of data) {
                    if (r.amostras && r.amostras.length > 0) {
                        for (let i = 0; i < r.amostras.length; i++) {
                            const amostra = r.amostras[i];
                            const date = new Date(r.data + 'T03:00:00Z');
                            const formattedDate = date.toLocaleDateString('pt-BR');

                            const somaFases = (amostra.fase1 || 0) + (amostra.fase2 || 0) + (amostra.fase3 || 0) + (amostra.fase4 || 0) + (amostra.fase5 || 0);
                            const resultadoAmostra = (somaFases / divisor).toFixed(2).replace('.', ',');

                            const row = [
                                `${r.codigo} - ${r.fazenda}`,
                                r.talhao,
                                formattedDate,
                                r.variedade,
                                r.adulto ? 'Sim' : 'Não',
                                i + 1,
                                amostra.fase1 || 0,
                                amostra.fase2 || 0,
                                amostra.fase3 || 0,
                                amostra.fase4 || 0,
                                amostra.fase5 || 0,
                                resultadoAmostra
                            ];
                            currentY = await checkPageBreak(doc, currentY, title);
                            currentY = drawRow(doc, row, currentY, false, false, columnWidths);
                        }
                    }
                }
            }

            generatePdfFooter(doc, filters.generatedBy);
            doc.end();
        } catch (error) {
            console.error("Erro ao gerar PDF de Cigarrinha (Amostragem):", error);
            if (!res.headersSent) {
                res.status(500).send(`Erro ao gerar relatório: ${error.message}`);
            } else {
                doc.end();
            }
        }
    });

    app.get('/reports/cigarrinha-amostragem/csv', async (req, res) => {
        try {
            const { tipoRelatorio = 'detalhado' } = req.query;
            const data = await getFilteredData('cigarrinhaAmostragem', req.query);
            if (data.length === 0) return res.status(404).send('Nenhum dado encontrado para os filtros selecionados.');

            const filename = `relatorio_cigarrinha_amostragem_${tipoRelatorio}_${Date.now()}.csv`;
            const filePath = path.join(os.tmpdir(), filename);

            let header, records;

            if (tipoRelatorio === 'resumido') {
                header = [
                    { id: 'data', title: 'Data' }, { id: 'fazenda', title: 'Fazenda' }, { id: 'talhao', title: 'Talhão' }, { id: 'variedade', title: 'Variedade' },
                    { id: 'fase1', title: 'Fase 1 (Soma)' }, { id: 'fase2', title: 'Fase 2 (Soma)' }, { id: 'fase3', title: 'Fase 3 (Soma)' },
                    { id: 'fase4', title: 'Fase 4 (Soma)' }, { id: 'fase5', title: 'Fase 5 (Soma)' }
                ];

                const groupedData = data.reduce((acc, r) => {
                    const date = new Date(r.data + 'T03:00:00Z');
                    const formattedDate = date.toLocaleDateString('pt-BR');
                    const key = `${formattedDate}|${r.codigo}|${r.fazenda}|${r.talhao}`;

                    if (!acc[key]) {
                        acc[key] = {
                            data: r.data, // Preserva a data original para ordenação
                            formattedDate: formattedDate,
                            codigo: r.codigo,
                            fazenda: `${r.codigo} - ${r.fazenda}`,
                            talhao: r.talhao,
                            variedade: r.variedade,
                            fase1: 0, fase2: 0, fase3: 0, fase4: 0, fase5: 0,
                        };
                    }
                    r.amostras.forEach(amostra => {
                        acc[key].fase1 += amostra.fase1 || 0;
                        acc[key].fase2 += amostra.fase2 || 0;
                        acc[key].fase3 += amostra.fase3 || 0;
                        acc[key].fase4 += amostra.fase4 || 0;
                        acc[key].fase5 += amostra.fase5 || 0;
                    });
                    return acc;
                }, {});

                let summarizedData = Object.values(groupedData);
                summarizedData.sort(sortByDateAndFazenda);

                records = summarizedData.map(rec => ({
                    data: rec.formattedDate,
                    fazenda: rec.fazenda,
                    talhao: rec.talhao,
                    variedade: rec.variedade,
                    fase1: rec.fase1,
                    fase2: rec.fase2,
                    fase3: rec.fase3,
                    fase4: rec.fase4,
                    fase5: rec.fase5
                }));

            } else if (tipoRelatorio === 'final') {
                header = [
                    { id: 'fazenda', title: 'Fazenda' }, { id: 'data', title: 'Data' }, { id: 'variedade', title: 'Variedade' },
                    { id: 'fase1', title: 'Fase1' }, { id: 'fase2', title: 'Fase2' }, { id: 'fase3', title: 'Fase3' },
                    { id: 'fase4', title: 'Fase4' }, { id: 'fase5', title: 'Fase5' }, { id: 'resultadoFinal', title: 'Resultado Final' }
                ];

                records = data.map(r => {
                    const date = new Date(r.data + 'T03:00:00Z');
                    const formattedDate = date.toLocaleDateString('pt-BR');

                    const totalFases = r.amostras.reduce((acc, amostra) => {
                        acc.f1 += amostra.fase1 || 0;
                        acc.f2 += amostra.fase2 || 0;
                        acc.f3 += amostra.fase3 || 0;
                        acc.f4 += amostra.fase4 || 0;
                        acc.f5 += amostra.fase5 || 0;
                        return acc;
                    }, { f1: 0, f2: 0, f3: 0, f4: 0, f5: 0 });

                    return {
                        fazenda: `${r.codigo} - ${r.fazenda}`,
                        data: formattedDate,
                        variedade: r.variedade,
                        fase1: totalFases.f1,
                        fase2: totalFases.f2,
                        fase3: totalFases.f3,
                        fase4: totalFases.f4,
                        fase5: totalFases.f5,
                        resultadoFinal: (r.resultado || 0).toFixed(2).replace('.', ',')
                    };
                });

            } else { // Detalhado
                header = [
                    { id: 'fazenda', title: 'Fazenda' }, { id: 'talhao', title: 'Talhão' }, { id: 'data', title: 'Data' }, { id: 'variedade', title: 'Variedade' },
                    { id: 'adulto', title: 'Adulto Presente'}, { id: 'numeroAmostra', title: 'Nº Amostra' }, { id: 'fase1', title: 'Fase 1' }, { id: 'fase2', title: 'Fase 2' },
                    { id: 'fase3', title: 'Fase 3' }, { id: 'fase4', title: 'Fase 4' }, { id: 'fase5', title: 'Fase 5' },
                    { id: 'resultadoAmostra', title: 'Resultado Amostra'}
                ];
                records = [];
                const divisor = parseInt(req.query.divisor, 10) || parseInt(data[0]?.divisor || '5', 10);

                data.forEach(lancamento => {
                    if (lancamento.amostras && lancamento.amostras.length > 0) {
                        lancamento.amostras.forEach((amostra, index) => {
                            const date = new Date(lancamento.data + 'T03:00:00Z');
                            const formattedDate = date.toLocaleDateString('pt-BR');
                            const somaFases = (amostra.fase1 || 0) + (amostra.fase2 || 0) + (amostra.fase3 || 0) + (amostra.fase4 || 0) + (amostra.fase5 || 0);
                            const resultadoAmostra = (somaFases / divisor).toFixed(2).replace('.', ',');

                            records.push({
                                fazenda: `${lancamento.codigo} - ${lancamento.fazenda}`, talhao: lancamento.talhao, data: formattedDate,
                                variedade: lancamento.variedade, adulto: lancamento.adulto ? 'Sim' : 'Não', numeroAmostra: index + 1, fase1: amostra.fase1 || 0,
                                fase2: amostra.fase2 || 0, fase3: amostra.fase3 || 0, fase4: amostra.fase4 || 0, fase5: amostra.fase5 || 0,
                                resultadoAmostra: resultadoAmostra
                            });
                        });
                    }
                });
            }

            const csvWriter = createObjectCsvWriter({ path: filePath, header: header, fieldDelimiter: ';' });
            await csvWriter.writeRecords(records);
            res.download(filePath);
        } catch (error) {
            console.error("Erro ao gerar CSV de Cigarrinha (Amostragem):", error);
            res.status(500).send('Erro ao gerar relatório.');
        }
    });

    app.get('/reports/cigarrinha/csv', async (req, res) => {
        try {
            const data = await getFilteredData('cigarrinha', req.query);
            if (data.length === 0) return res.status(404).send('Nenhum dado encontrado.');

            const filePath = path.join(os.tmpdir(), `cigarrinha_${Date.now()}.csv`);
            const csvWriter = createObjectCsvWriter({
                path: filePath,
                header: [
                    {id: 'data', title: 'Data'}, {id: 'fazenda', title: 'Fazenda'}, {id: 'talhao', title: 'Talhão'},
                    {id: 'variedade', title: 'Variedade'}, {id: 'fase1', title: 'Fase 1'}, {id: 'fase2', title: 'Fase 2'},
                    {id: 'fase3', title: 'Fase 3'}, {id: 'fase4', title: 'Fase 4'}, {id: 'fase5', title: 'Fase 5'},
                    {id: 'adulto', title: 'Adulto Presente'}, {id: 'resultado', title: 'Resultado'}
                ]
            });

            const fazendasSnapshot = await db.collection('fazendas').where('companyId', '==', req.query.companyId).get();
            const fazendasData = {};
            fazendasSnapshot.forEach(docSnap => {
                fazendasData[docSnap.data().code] = docSnap.data();
            });

            const records = data.map(r => {
                const farm = fazendasData[r.codigo];
                const talhao = farm?.talhoes.find(t => t.name.toUpperCase() === r.talhao.toUpperCase());
                const date = new Date(r.data + 'T03:00:00Z');
                const formattedDate = date.toLocaleDateString('pt-BR');
                return {
                    ...r,
                    data: formattedDate,
                    fazenda: `${r.codigo} - ${r.fazenda}`,
                    variedade: talhao?.variedade || 'N/A',
                    adulto: r.adulto ? 'Sim' : 'Não',
                    resultado: r.resultado
                };
            });

            await csvWriter.writeRecords(records);
            res.download(filePath);
        } catch (error) {
            console.error("Erro ao gerar CSV de Cigarrinha:", error);
            res.status(500).send('Erro ao gerar relatório.');
        }
    });

    app.get('/reports/colheita/pdf', async (req, res) => {
        const doc = new PDFDocument({ margin: 30, size: 'A4', layout: 'landscape', bufferPages: true });
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename=relatorio_colheita_custom.pdf`);
        doc.pipe(res);

        try {
            const { planId, selectedColumns, generatedBy, companyId } = req.query;
            const selectedCols = JSON.parse(selectedColumns || '{}');

            if (!planId) {
                await generatePdfHeader(doc, 'Relatório Customizado de Colheita');
                doc.text('Nenhum plano de colheita selecionado.');
                generatePdfFooter(doc, generatedBy);
                doc.end();
                return;
            }
             if (!companyId) {
                await generatePdfHeader(doc, 'Relatório Customizado de Colheita');
                doc.text('ID da empresa não fornecido.');
                generatePdfFooter(doc, generatedBy);
                doc.end();
                return;
            }

            const harvestPlanDoc = await db.collection('harvestPlans').doc(planId).get();
            if (!harvestPlanDoc.exists || harvestPlanDoc.data().companyId !== companyId) {
                await generatePdfHeader(doc, 'Relatório Customizado de Colheita');
                doc.text('Plano de colheita não encontrado ou não pertence a esta empresa.');
                generatePdfFooter(doc, generatedBy);
                doc.end();
                return;
            }

            const harvestPlan = harvestPlanDoc.data();
            const fazendasSnapshot = await db.collection('fazendas').where('companyId', '==', companyId).get();
            const fazendasData = {};
            fazendasSnapshot.forEach(docSnap => {
                const data = docSnap.data();
                fazendasData[data.code] = { id: docSnap.id, ...data };
            });

            const title = `Relatório de Colheita - ${harvestPlan.frontName}`;
            let currentY = await generatePdfHeader(doc, title);

            const allPossibleHeadersConfig = [
                { id: 'seq', title: 'Seq.', minWidth: 35 },
                { id: 'fazenda', title: 'Fazenda', minWidth: 120 },
                { id: 'talhoes', title: 'Talhões', minWidth: 160 },
                { id: 'area', title: 'Área (ha)', minWidth: 50 },
                { id: 'producao', title: 'Prod. (ton)', minWidth: 60 },
                { id: 'variedade', title: 'Variedade', minWidth: 130 },
                { id: 'idade', title: 'Idade (m)', minWidth: 55 },
                { id: 'atr', title: 'ATR', minWidth: 40 },
                { id: 'maturador', title: 'Matur.', minWidth: 60 },
                { id: 'diasAplicacao', title: 'Dias Aplic.', minWidth: 70 },
                { id: 'distancia', title: 'KM', minWidth: 40 },
                { id: 'entrada', title: 'Entrada', minWidth: 65 },
                { id: 'saida', title: 'Saída', minWidth: 65 }
            ];

            let finalHeaders = [];
            const initialFixedHeaders = ['seq', 'fazenda', 'area', 'producao'];
            const finalFixedHeaders = ['entrada', 'saida'];
            
            initialFixedHeaders.forEach(id => {
                const header = allPossibleHeadersConfig.find(h => h.id === id);
                if (header) finalHeaders.push(header);
            });

            if (selectedCols['talhoes']) {
                const header = allPossibleHeadersConfig.find(h => h.id === 'talhoes');
                if (header) finalHeaders.push(header);
            }

            allPossibleHeadersConfig.forEach(header => {
                if (selectedCols[header.id] && !initialFixedHeaders.includes(header.id) && !finalFixedHeaders.includes(header.id) && header.id !== 'talhoes') {
                    finalHeaders.push(header);
                }
            });

            finalFixedHeaders.forEach(id => {
                const header = allPossibleHeadersConfig.find(h => h.id === id);
                if (header) finalHeaders.push(header);
            });

            const headersText = finalHeaders.map(h => h.title);

            const pageWidth = doc.page.width - doc.page.margins.left - doc.page.margins.right;
            let totalMinWidth = 0;
            let flexibleColumnsCount = 0;

            finalHeaders.forEach(header => {
                totalMinWidth += header.minWidth;
                if (['fazenda', 'talhoes', 'variedade'].includes(header.id)) {
                    flexibleColumnsCount++;
                }
            });

            let remainingWidth = pageWidth - totalMinWidth;
            let flexibleColumnExtraWidth = flexibleColumnsCount > 0 ? remainingWidth / flexibleColumnsCount : 0;

            let finalColumnWidths = finalHeaders.map(header => {
                let width = header.minWidth;
                if (['fazenda', 'talhoes', 'variedade'].includes(header.id)) {
                    width += flexibleColumnExtraWidth;
                }
                return width;
            });

            const currentTotalWidth = finalColumnWidths.reduce((sum, w) => sum + w, 0);
            const difference = pageWidth - currentTotalWidth;
            if (difference !== 0 && flexibleColumnsCount > 0) {
                const firstFlexibleIndex = finalHeaders.findIndex(h => ['fazenda', 'talhoes', 'variedade'].includes(h.id));
                if (firstFlexibleIndex !== -1) {
                    finalColumnWidths[firstFlexibleIndex] += difference;
                }
            }


            const rowHeight = 18;
            const textPadding = 5;

            currentY = drawRow(doc, headersText, currentY, true, false, finalColumnWidths, textPadding, rowHeight, finalHeaders);

            let grandTotalProducao = 0;
            let grandTotalArea = 0;
            let currentDate = new Date(harvestPlan.startDate + 'T03:00:00Z');
            const dailyTon = parseFloat(harvestPlan.dailyRate) || 1;
            const closedTalhaoIds = new Set(harvestPlan.closedTalhaoIds || []);

            for (let i = 0; i < harvestPlan.sequence.length; i++) {
                const group = harvestPlan.sequence[i];
                
                const isGroupClosed = group.plots.every(p => closedTalhaoIds.has(p.talhaoId));
                
                if (!isGroupClosed) {
                    grandTotalProducao += group.totalProducao;
                    grandTotalArea += group.totalArea;
                }

                const diasNecessarios = dailyTon > 0 ? Math.ceil(group.totalProducao / dailyTon) : 0;
                const dataEntrada = new Date(currentDate.getTime());
                
                let dataSaida = new Date(dataEntrada.getTime());
                dataSaida.setDate(dataSaida.getDate() + (diasNecessarios > 0 ? diasNecessarios - 1 : 0));

                if (!isGroupClosed) {
                    currentDate = new Date(dataSaida.getTime());
                    currentDate.setDate(currentDate.getDate() + 1);
                }

                let totalAgeInDays = 0, plotsWithDate = 0;
                let totalDistancia = 0, plotsWithDistancia = 0;
                const allVarieties = new Set();

                group.plots.forEach(plot => {
                    const farm = fazendasData[group.fazendaCodigo];
                    const talhao = farm?.talhoes.find(t => t.id === plot.talhaoId);
                    if (talhao) {
                        if (talhao.dataUltimaColheita) {
                            const dataUltima = new Date(talhao.dataUltimaColheita + 'T03:00:00Z');
                            if (!isNaN(dataUltima)) {
                                totalAgeInDays += Math.abs(dataEntrada - dataUltima);
                                plotsWithDate++;
                            }
                        }
                        if (talhao.variedade) allVarieties.add(talhao.variedade);
                        if (typeof talhao.distancia === 'number') {
                            totalDistancia += talhao.distancia;
                            plotsWithDistancia++;
                        }
                    }
                });

                const idadeMediaMeses = plotsWithDate > 0 ? ((totalAgeInDays / plotsWithDate) / (1000 * 60 * 60 * 24 * 30)).toFixed(1) : 'N/A';
                const avgDistancia = plotsWithDistancia > 0 ? (totalDistancia / plotsWithDistancia).toFixed(2) : 'N/A';
                
                let diasAplicacao = 'N/A';
                if (group.maturadorDate) {
                    try {
                        const today = new Date();
                        const applicationDate = new Date(group.maturadorDate + 'T03:00:00Z');
                        const diffTime = today - applicationDate;
                        if (diffTime >= 0) {
                            diasAplicacao = Math.floor(diffTime / (1000 * 60 * 60 * 24));
                        }
                    } catch (e) { diasAplicacao = 'N/A'; }
                }

                const rowDataMap = {
                    seq: i + 1,
                    fazenda: `${group.fazendaCodigo} - ${group.fazendaName} ${isGroupClosed ? '(ENCERRADO)' : ''}`,
                    talhoes: group.plots.map(p => p.talhaoName).join(', '),
                    area: formatNumber(group.totalArea),
                    producao: formatNumber(group.totalProducao),
                    variedade: Array.from(allVarieties).join(', ') || 'N/A',
                    idade: idadeMediaMeses,
                    atr: group.atr || 'N/A',
                    maturador: group.maturador || 'N/A',
                    diasAplicacao: diasAplicacao,
                    distancia: avgDistancia,
                    entrada: dataEntrada.toLocaleDateString('pt-BR'),
                    saida: dataSaida.toLocaleDateString('pt-BR')
                };
                
                const rowData = finalHeaders.map(h => rowDataMap[h.id]);

                currentY = await checkPageBreak(doc, currentY, title);
                currentY = drawRow(doc, rowData, currentY, false, false, finalColumnWidths, textPadding, rowHeight, finalHeaders, isGroupClosed);
            }

            currentY = await checkPageBreak(doc, currentY, title, 40);
            doc.y = currentY;
            
            const totalRowData = new Array(finalHeaders.length).fill('');
            const fazendaIndex = finalHeaders.findIndex(h => h.id === 'fazenda');
            const areaIndex = finalHeaders.findIndex(h => h.id === 'area');
            const prodIndex = finalHeaders.findIndex(h => h.id === 'producao');

            if (fazendaIndex !== -1) {
                totalRowData[fazendaIndex] = 'Total Geral (Ativo)';
            } else {
                totalRowData[1] = 'Total Geral (Ativo)';
            }

            if (areaIndex !== -1) {
                totalRowData[areaIndex] = formatNumber(grandTotalArea);
            }
            if (prodIndex !== -1) {
                totalRowData[prodIndex] = formatNumber(grandTotalProducao);
            }

            drawRow(doc, totalRowData, currentY, false, true, finalColumnWidths, textPadding, rowHeight, finalHeaders);

            generatePdfFooter(doc, generatedBy);
            doc.end();
        } catch (error) {
            console.error("Erro no PDF de Colheita:", error);
            if (!res.headersSent) {
                res.status(500).send(`Erro ao gerar relatório: ${error.message}`);
            } else {
                doc.end();
            }
        }
    });

    app.get('/reports/colheita/mensal/csv', async (req, res) => {
        try {
            const { planId, companyId } = req.query;
            if (!planId) return res.status(400).send('Nenhum plano de colheita selecionado.');
            if (!companyId) return res.status(400).send('O ID da empresa é obrigatório.');

            const harvestPlanDoc = await db.collection('harvestPlans').doc(planId).get();
            if (!harvestPlanDoc.exists || harvestPlanDoc.data().companyId !== companyId) {
                return res.status(404).send('Plano de colheita não encontrado ou não pertence a esta empresa.');
            }

            const harvestPlan = harvestPlanDoc.data();
            const monthlyTotals = {};
            let currentDate = new Date(harvestPlan.startDate + 'T03:00:00Z');
            const dailyTon = parseFloat(harvestPlan.dailyRate) || 1;
            const closedTalhaoIds = new Set(harvestPlan.closedTalhaoIds || []);

            harvestPlan.sequence.forEach(group => {
                const isGroupClosed = group.plots.every(p => closedTalhaoIds.has(p.talhaoId));
                if(isGroupClosed) return;

                let producaoRestante = group.totalProducao;
                while (producaoRestante > 0) {
                    const monthKey = `${currentDate.getFullYear()}-${String(currentDate.getMonth() + 1).padStart(2, '0')}`;
                    if (!monthlyTotals[monthKey]) {
                        monthlyTotals[monthKey] = 0;
                    }
                    monthlyTotals[monthKey] += Math.min(producaoRestante, dailyTon);
                    producaoRestante -= dailyTon;
                    currentDate.setDate(currentDate.getDate() + 1);
                }
            });

            const filePath = path.join(os.tmpdir(), `previsao_mensal_${Date.now()}.csv`);
            const csvWriter = createObjectCsvWriter({
                path: filePath,
                header: [
                    { id: 'mes', title: 'Mês/Ano' },
                    { id: 'producao', title: 'Produção Total (ton)' }
                ]
            });

            const records = Object.keys(monthlyTotals).sort().map(monthKey => {
                const [year, month] = monthKey.split('-');
                const monthName = new Date(year, month - 1, 1).toLocaleString('pt-BR', { month: 'long' });
                return {
                    mes: `${monthName.charAt(0).toUpperCase() + monthName.slice(1)}/${year}`,
                    producao: monthlyTotals[monthKey].toFixed(2)
                };
            });

            await csvWriter.writeRecords(records);
            res.download(filePath);
        } catch (error) {
            console.error("Erro ao gerar CSV de Previsão Mensal:", error);
            res.status(500).send('Erro ao gerar relatório.');
        }
    });

    app.get('/reports/colheita/csv', async (req, res) => {
        try {
            const { planId, selectedColumns, companyId } = req.query;
            const selectedCols = JSON.parse(selectedColumns || '{}');
            if (!planId) return res.status(400).send('Nenhum plano de colheita selecionado.');
            if (!companyId) return res.status(400).send('O ID da empresa é obrigatório.');

            const harvestPlanDoc = await db.collection('harvestPlans').doc(planId).get();
            if (!harvestPlanDoc.exists || harvestPlanDoc.data().companyId !== companyId) {
                return res.status(404).send('Plano de colheita não encontrado ou não pertence a esta empresa.');
            }

            const harvestPlan = harvestPlanDoc.data();
            const fazendasSnapshot = await db.collection('fazendas').where('companyId', '==', companyId).get();
            const fazendasData = {};
            fazendasSnapshot.forEach(docSnap => {
                const data = docSnap.data();
                fazendasData[data.code] = { id: docSnap.id, ...data };
            });

            const allPossibleHeaders = [
                { id: 'seq', title: 'Seq.' }, { id: 'fazenda', title: 'Fazenda' },
                { id: 'talhoes', title: 'Talhões' }, { id: 'area', title: 'Área (ha)' },
                { id: 'producao', title: 'Produção (ton)' }, { id: 'variedade', title: 'Variedade' },
                { id: 'idade', title: 'Idade (m)' }, { id: 'atr', title: 'ATR' },
                { id: 'maturador', title: 'Maturador' }, { id: 'diasAplicacao', title: 'Dias Aplic.' },
                { id: 'distancia', title: 'KM' }, { id: 'entrada', title: 'Entrada' },
                { id: 'saida', title: 'Saída' }
            ];

            let finalHeaders = allPossibleHeaders.filter(h =>
                ['seq', 'fazenda', 'area', 'producao', 'entrada', 'saida'].includes(h.id) || selectedCols[h.id]
            );

            const records = [];
            let currentDate = new Date(harvestPlan.startDate + 'T03:00:00Z');
            const dailyTon = parseFloat(harvestPlan.dailyRate) || 1;
            const closedTalhaoIds = new Set(harvestPlan.closedTalhaoIds || []);

            for (let i = 0; i < harvestPlan.sequence.length; i++) {
                const group = harvestPlan.sequence[i];
                const isGroupClosed = group.plots.every(p => closedTalhaoIds.has(p.talhaoId));

                const diasNecessarios = dailyTon > 0 ? Math.ceil(group.totalProducao / dailyTon) : 0;
                const dataEntrada = new Date(currentDate.getTime());
                let dataSaida = new Date(dataEntrada.getTime());
                dataSaida.setDate(dataSaida.getDate() + (diasNecessarios > 0 ? diasNecessarios - 1 : 0));

                if (!isGroupClosed) {
                    currentDate = new Date(dataSaida.getTime());
                    currentDate.setDate(currentDate.getDate() + 1);
                }

                // Cálculos auxiliares
                let totalAgeInDays = 0, plotsWithDate = 0, totalDistancia = 0, plotsWithDistancia = 0;
                const allVarieties = new Set();
                group.plots.forEach(plot => {
                    const farm = fazendasData[group.fazendaCodigo];
                    const talhao = farm?.talhoes.find(t => t.id === plot.talhaoId);
                    if (talhao) {
                        if (talhao.dataUltimaColheita) {
                            const dataUltima = new Date(talhao.dataUltimaColheita + 'T03:00:00Z');
                            if (!isNaN(dataUltima)) { totalAgeInDays += Math.abs(dataEntrada - dataUltima); plotsWithDate++; }
                        }
                        if (talhao.variedade) allVarieties.add(talhao.variedade);
                        if (typeof talhao.distancia === 'number') { totalDistancia += talhao.distancia; plotsWithDistancia++; }
                    }
                });
                const idadeMediaMeses = plotsWithDate > 0 ? ((totalAgeInDays / plotsWithDate) / (1000 * 60 * 60 * 24 * 30)).toFixed(1) : 'N/A';
                const avgDistancia = plotsWithDistancia > 0 ? (totalDistancia / plotsWithDistancia).toFixed(2) : 'N/A';
                let diasAplicacao = 'N/A';
                if (group.maturadorDate) {
                    try {
                        const diffTime = new Date() - new Date(group.maturadorDate + 'T03:00:00Z');
                        if (diffTime >= 0) diasAplicacao = Math.floor(diffTime / (1000 * 60 * 60 * 24));
                    } catch (e) {}
                }

                const record = {
                    seq: i + 1,
                    fazenda: `${group.fazendaCodigo} - ${group.fazendaName} ${isGroupClosed ? '(ENCERRADO)' : ''}`,
                    talhoes: group.plots.map(p => p.talhaoName).join(', '),
                    area: group.totalArea.toFixed(2),
                    producao: group.totalProducao.toFixed(2),
                    variedade: Array.from(allVarieties).join(', ') || 'N/A',
                    idade: idadeMediaMeses,
                    atr: group.atr || 'N/A',
                    maturador: group.maturador || 'N/A',
                    diasAplicacao: diasAplicacao,
                    distancia: avgDistancia,
                    entrada: dataEntrada.toLocaleDateString('pt-BR'),
                    saida: dataSaida.toLocaleDateString('pt-BR')
                };
                records.push(record);
            }

            const filePath = path.join(os.tmpdir(), `relatorio_colheita_${Date.now()}.csv`);
            const csvWriter = createObjectCsvWriter({ path: filePath, header: finalHeaders });
            await csvWriter.writeRecords(records);
            res.download(filePath);
        } catch (error) {
            console.error("Erro ao gerar CSV de Colheita Detalhado:", error);
            res.status(500).send('Erro ao gerar relatório.');
        }
    });

    app.get('/reports/colheita/mensal/pdf', async (req, res) => {
        const doc = new PDFDocument({ margin: 30, size: 'A4', bufferPages: true });
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename=previsao_mensal_colheita.pdf`);
        doc.pipe(res);

        try {
            const { planId, generatedBy, companyId } = req.query;
            if (!planId) throw new Error('Nenhum plano de colheita selecionado.');
            if (!companyId) throw new Error('O ID da empresa é obrigatório.');

            const harvestPlanDoc = await db.collection('harvestPlans').doc(planId).get();
            if (!harvestPlanDoc.exists || harvestPlanDoc.data().companyId !== companyId) {
                 throw new Error('Plano de colheita não encontrado ou não pertence a esta empresa.');
            }

            const harvestPlan = harvestPlanDoc.data();
            const monthlyTotals = {};
            let currentDate = new Date(harvestPlan.startDate + 'T03:00:00Z');
            const dailyTon = parseFloat(harvestPlan.dailyRate) || 1;
            const closedTalhaoIds = new Set(harvestPlan.closedTalhaoIds || []);

            harvestPlan.sequence.forEach(group => {
                if (group.plots.every(p => closedTalhaoIds.has(p.talhaoId))) return;
                let producaoRestante = group.totalProducao;
                while (producaoRestante > 0) {
                    const monthKey = `${currentDate.getFullYear()}-${String(currentDate.getMonth() + 1).padStart(2, '0')}`;
                    if (!monthlyTotals[monthKey]) {
                        monthlyTotals[monthKey] = 0;
                    }
                    monthlyTotals[monthKey] += Math.min(producaoRestante, dailyTon);
                    producaoRestante -= dailyTon;
                    currentDate.setDate(currentDate.getDate() + 1);
                }
            });

            const title = `Previsão Mensal de Colheita - ${harvestPlan.frontName}`;
            let currentY = await generatePdfHeader(doc, title);

            const headers = ['Mês/Ano', 'Produção Total (ton)'];
            const columnWidths = [250, 250];

            currentY = drawRow(doc, headers, currentY, true, false, columnWidths);

            const sortedMonths = Object.keys(monthlyTotals).sort();
            for (const monthKey of sortedMonths) {
                currentY = await checkPageBreak(doc, currentY, title);
                const [year, month] = monthKey.split('-');
                const monthName = new Date(year, month - 1, 1).toLocaleString('pt-BR', { month: 'long' });
                const rowData = [
                    `${monthName.charAt(0).toUpperCase() + monthName.slice(1)}/${year}`,
                    formatNumber(monthlyTotals[monthKey])
                ];
                currentY = drawRow(doc, rowData, currentY, false, false, columnWidths);
            }

            generatePdfFooter(doc, generatedBy);
            doc.end();
        } catch (error) {
            console.error("Erro ao gerar PDF de Previsão Mensal:", error);
            if (!res.headersSent) res.status(500).send(`Erro ao gerar relatório: ${error.message}`);
            else doc.end();
        }
    });

    app.get('/reports/monitoramento/pdf', async (req, res) => {
        const doc = new PDFDocument({ margin: 30, size: 'A4', layout: 'landscape', bufferPages: true });
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename=relatorio_monitoramento.pdf`);
        doc.pipe(res);

        try {
            const { inicio, fim, fazendaCodigo, generatedBy, companyId } = req.query;
            if (!companyId) {
                // Renderiza um PDF de erro se o companyId não for fornecido
                await generatePdfHeader(doc, 'Erro');
                doc.text('O ID da empresa não foi fornecido.');
                doc.end();
                return;
            }
            let query = db.collection('armadilhas').where('companyId', '==', companyId).where('status', '==', 'Coletada');

            if (inicio) query = query.where('dataColeta', '>=', new Date(inicio));
            if (fim) query = query.where('dataColeta', '<=', new Date(fim));

            const snapshot = await query.get();
            let data = [];
            snapshot.forEach(doc => data.push({ id: doc.id, ...doc.data() }));

            const title = 'Relatório de Monitoramento de Armadilhas';
            let currentY = await generatePdfHeader(doc, title, companyId);

            if (data.length === 0) {
                doc.text('Nenhuma armadilha coletada encontrada para os filtros selecionados.');
                generatePdfFooter(doc, generatedBy);
                return doc.end();
            }

            const geojsonData = await getShapefileData(companyId);
            
            const enrichedData = data.map(trap => {
                const talhaoProps = findTalhaoForTrap(trap, geojsonData);
                return {
                    ...trap,
                    fazendaNome: talhaoProps?.NM_IMOVEL || 'N/A',
                    fazendaCodigoShape: talhaoProps?.CD_FAZENDA || 'N/A',
                    talhaoNome: talhaoProps?.CD_TALHAO || 'N/A'
                };
            });

            let finalData = enrichedData;
            if (fazendaCodigo) {
                finalData = enrichedData.filter(d => d.fazendaCodigoShape === fazendaCodigo);
            }

            const headers = ['Fazenda', 'Talhão', 'Data Instalação', 'Data Coleta', 'Qtd. Mariposas'];
            const columnWidths = [200, 100, 120, 120, 120];
            const rowHeight = 18;
            const textPadding = 5;

            currentY = drawRow(doc, headers, currentY, true, false, columnWidths, textPadding, rowHeight);

            for (const trap of finalData) {
                currentY = await checkPageBreak(doc, currentY, title);
                const rowData = [
                    `${trap.fazendaCodigoShape} - ${trap.fazendaNome}`,
                    trap.talhaoNome,
                    trap.dataInstalacao.toDate().toLocaleString('pt-BR'),
                    trap.dataColeta.toDate().toLocaleString('pt-BR'),
                    trap.contagemMariposas || 0
                ];
                currentY = drawRow(doc, rowData, currentY, false, false, columnWidths, textPadding, rowHeight);
            }

            generatePdfFooter(doc, generatedBy);
            doc.end();
        } catch (error) {
            console.error("Erro ao gerar PDF de Monitoramento:", error);
            if (!res.headersSent) res.status(500).send(`Erro ao gerar relatório: ${error.message}`);
            else doc.end();
        }
    });

    app.get('/reports/armadilhas/pdf', async (req, res) => {
        const doc = new PDFDocument({ margin: 30, size: 'A4', layout: 'landscape', bufferPages: true });
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename=relatorio_armadilhas.pdf`);
        doc.pipe(res);

        try {
            const { inicio, fim, fazendaCodigo, generatedBy, companyId } = req.query;
            if (!companyId) {
                await generatePdfHeader(doc, 'Erro');
                doc.text('O ID da empresa não foi fornecido.');
                doc.end();
                return;
            }
            let query = db.collection('armadilhas').where('companyId', '==', companyId).where('status', '==', 'Coletada');
            
            if (inicio) query = query.where('dataColeta', '>=', admin.firestore.Timestamp.fromDate(new Date(inicio + 'T00:00:00')));
            if (fim) query = query.where('dataColeta', '<=', admin.firestore.Timestamp.fromDate(new Date(fim + 'T23:59:59')));

            const snapshot = await query.get();
            let data = [];
            snapshot.forEach(doc => data.push({ id: doc.id, ...doc.data() }));

            const title = 'Relatório de Armadilhas Coletadas';

            if (data.length === 0) {
                await generatePdfHeader(doc, title, companyId);
                doc.text('Nenhuma armadilha coletada encontrada para os filtros selecionados.');
                generatePdfFooter(doc, generatedBy);
                return doc.end();
            }

            const usersSnapshot = await db.collection('users').where('companyId', '==', companyId).get();
            const usersMap = {};
            usersSnapshot.forEach(doc => {
                usersMap[doc.id] = doc.data().username || doc.data().email;
            });

            const geojsonData = await getShapefileData(companyId);
            
            let enrichedData = data.map(trap => {
                const talhaoProps = geojsonData ? findTalhaoForTrap(trap, geojsonData) : null;
                const dataInstalacao = safeToDate(trap.dataInstalacao);
                const dataColeta = safeToDate(trap.dataColeta);

                let diasEmCampo = 'N/A';
                if (dataInstalacao && dataColeta) {
                    const diffTime = Math.abs(dataColeta - dataInstalacao);
                    diasEmCampo = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                }

                const fazendaNome = findShapefileProp(talhaoProps, ['NM_IMOVEL', 'NM_FAZENDA', 'NOME_FAZEN', 'FAZENDA']) || trap.fazendaNome || 'N/A';
                const fundoAgricola = findShapefileProp(talhaoProps, ['FUNDO_AGR']) || trap.fazendaCode || 'N/A';
                const talhaoNome = findShapefileProp(talhaoProps, ['CD_TALHAO', 'COD_TALHAO', 'TALHAO']) || trap.talhaoNome || 'N/A';


                return {
                    ...trap,
                    fazendaNome: fazendaNome,
                    fundoAgricola: fundoAgricola,
                    talhaoNome: talhaoNome,
                    dataInstalacaoFmt: dataInstalacao ? dataInstalacao.toLocaleDateString('pt-BR') : 'N/A',
                    dataColetaFmt: dataColeta ? dataColeta.toLocaleDateString('pt-BR') : 'N/A',
                    diasEmCampo: diasEmCampo,
                    instaladoPorNome: usersMap[trap.instaladoPor] || 'Desconhecido',
                    coletadoPorNome: usersMap[trap.coletadoPor] || 'Desconhecido',
                };
            });

            if (fazendaCodigo) {
                const farmQuery = db.collection('fazendas').where('companyId', '==', companyId).where('code', '==', fazendaCodigo).limit(1);
                const farm = await farmQuery.get();
                if (!farm.empty) {
                    const farmName = farm.docs[0].data().name;
                    enrichedData = enrichedData.filter(d => d.fazendaNome === farmName);
                } else {
                    enrichedData = [];
                }
            }

            let currentY = await generatePdfHeader(doc, title);

            const headers = ['Fundo Agrícola', 'Fazenda', 'Talhão', 'Data Inst.', 'Data Coleta', 'Dias Campo', 'Qtd. Mariposas', 'Instalado Por', 'Coletado Por', 'Obs.'];
            const columnWidths = [90, 120, 60, 65, 65, 60, 75, 80, 80, 87];
            const rowHeight = 18;
            const textPadding = 5;

            currentY = drawRow(doc, headers, currentY, true, false, columnWidths, textPadding, rowHeight);

            for (const trap of enrichedData) {
                currentY = await checkPageBreak(doc, currentY, title);
                const rowData = [
                    trap.fundoAgricola,
                    trap.fazendaNome,
                    trap.talhaoNome,
                    trap.dataInstalacaoFmt,
                    trap.dataColetaFmt,
                    trap.diasEmCampo,
                    trap.contagemMariposas || 0,
                    trap.instaladoPorNome,
                    trap.coletadoPorNome,
                    trap.observacoes || ''
                ];
                currentY = drawRow(doc, rowData, currentY, false, false, columnWidths, textPadding, rowHeight);
            }

            generatePdfFooter(doc, generatedBy);
            doc.end();

        } catch (error) {
            console.error("Erro ao gerar PDF de Armadilhas:", error);
            if (!res.headersSent) res.status(500).send(`Erro ao gerar relatório: ${error.message}`);
            else doc.end();
        }
    });

    app.get('/reports/armadilhas/csv', async (req, res) => {
        try {
            const { inicio, fim, fazendaCodigo, companyId } = req.query;
            if (!companyId) {
                return res.status(400).send('O ID da empresa é obrigatório.');
            }
            let query = db.collection('armadilhas').where('companyId', '==', companyId).where('status', '==', 'Coletada');
            
            if (inicio) query = query.where('dataColeta', '>=', admin.firestore.Timestamp.fromDate(new Date(inicio + 'T00:00:00')));
            if (fim) query = query.where('dataColeta', '<=', admin.firestore.Timestamp.fromDate(new Date(fim + 'T23:59:59')));

            const snapshot = await query.get();
            let data = [];
            snapshot.forEach(doc => data.push({ id: doc.id, ...doc.data() }));

            if (data.length === 0) return res.status(404).send('Nenhum dado encontrado para os filtros selecionados.');

            const usersSnapshot = await db.collection('users').where('companyId', '==', companyId).get();
            const usersMap = {};
            usersSnapshot.forEach(doc => {
                usersMap[doc.id] = doc.data().username || doc.data().email;
            });

            const geojsonData = await getShapefileData(companyId);

            let enrichedData = data.map(trap => {
                const talhaoProps = findTalhaoForTrap(trap, geojsonData);
                const dataInstalacao = safeToDate(trap.dataInstalacao);
                const dataColeta = safeToDate(trap.dataColeta);

                let diasEmCampo = 'N/A';
                if (dataInstalacao && dataColeta) {
                    const diffTime = Math.abs(dataColeta - dataInstalacao);
                    diasEmCampo = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                }

                return {
                    fundoAgricola: findShapefileProp(talhaoProps, ['FUNDO_AGR']) || trap.fundoAgricola || 'N/A',
                    fazendaNome: findShapefileProp(talhaoProps, ['NM_IMOVEL', 'NM_FAZENDA', 'NOME_FAZEN', 'FAZENDA']) || trap.fazendaNome || 'N/A',
                    talhaoNome: findShapefileProp(talhaoProps, ['CD_TALHAO', 'COD_TALHAO', 'TALHAO']) || trap.talhaoNome || 'N/A',
                    dataInstalacao: dataInstalacao ? dataInstalacao.toLocaleDateString('pt-BR') : 'N/A',
                    dataColeta: dataColeta ? dataColeta.toLocaleDateString('pt-BR') : 'N/A',
                    diasEmCampo: diasEmCampo,
                    contagemMariposas: trap.contagemMariposas || 0,
                    instaladoPor: usersMap[trap.instaladoPor] || 'Desconhecido',
                    coletadoPor: usersMap[trap.coletadoPor] || 'Desconhecido',
                    observacoes: trap.observacoes || ''
                };
            });
            
            if (fazendaCodigo) {
                const farmQuery = db.collection('fazendas').where('companyId', '==', companyId).where('code', '==', fazendaCodigo).limit(1);
                const farm = await farmQuery.get();
                if (!farm.empty) {
                    const farmName = farm.docs[0].data().name;
                    enrichedData = enrichedData.filter(d => d.fazendaNome === farmName);
                } else {
                    enrichedData = [];
                }
            }

            const filePath = path.join(os.tmpdir(), `armadilhas_report_${Date.now()}.csv`);
            const csvWriter = createObjectCsvWriter({
                path: filePath,
                header: [
                    { id: 'fundoAgricola', title: 'Fundo Agrícola' },
                    { id: 'fazendaNome', title: 'Fazenda' },
                    { id: 'talhaoNome', title: 'Talhão' },
                    { id: 'dataInstalacao', title: 'Data Instalação' },
                    { id: 'dataColeta', title: 'Data Coleta' },
                    { id: 'diasEmCampo', title: 'Dias em Campo' },
                    { id: 'contagemMariposas', title: 'Qtd. Mariposas' },
                    { id: 'instaladoPor', title: 'Instalado Por' },
                    { id: 'coletadoPor', title: 'Coletado Por' },
                    { id: 'observacoes', title: 'Observações' }
                ]
            });

            await csvWriter.writeRecords(enrichedData);
            res.download(filePath);

        } catch (error) {
            console.error("Erro ao gerar CSV de Armadilhas:", error);
            res.status(500).send('Erro ao gerar relatório.');
        }
    });


    app.get('/reports/armadilhas-ativas/pdf', async (req, res) => {
        const doc = new PDFDocument({ margin: 30, size: 'A4', layout: 'landscape', bufferPages: true });
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename=relatorio_armadilhas_instaladas.pdf`);
        doc.pipe(res);

        try {
            const { inicio, fim, fazendaCodigo, generatedBy, companyId } = req.query;
            if (!companyId) {
                await generatePdfHeader(doc, 'Erro');
                doc.text('O ID da empresa não foi fornecido.');
                doc.end();
                return;
            }
            let query = db.collection('armadilhas').where('companyId', '==', companyId).where('status', '==', 'Ativa');
            
            if (inicio) query = query.where('dataInstalacao', '>=', admin.firestore.Timestamp.fromDate(new Date(inicio + 'T00:00:00')));
            if (fim) query = query.where('dataInstalacao', '<=', admin.firestore.Timestamp.fromDate(new Date(fim + 'T23:59:59')));

            const snapshot = await query.get();
            let data = [];
            snapshot.forEach(doc => data.push({ id: doc.id, ...doc.data() }));

            const title = 'Relatório de Armadilhas Instaladas (Ativas)';

            if (data.length === 0) {
                await generatePdfHeader(doc, title, companyId);
                doc.text('Nenhuma armadilha ativa encontrada para os filtros selecionados.');
                generatePdfFooter(doc, generatedBy);
                return doc.end();
            }

            const usersSnapshot = await db.collection('users').where('companyId', '==', companyId).get();
            const usersMap = {};
            usersSnapshot.forEach(doc => {
                usersMap[doc.id] = doc.data().username || doc.data().email;
            });
            
            const geojsonData = await getShapefileData(companyId);

            let enrichedData = data.map(trap => {
                const talhaoProps = geojsonData ? findTalhaoForTrap(trap, geojsonData) : null;
                const dataInstalacao = safeToDate(trap.dataInstalacao);

                let diasEmCampo = 'N/A';
                let previsaoRetiradaFmt = 'N/A';

                if (dataInstalacao) {
                    const diffTime = Math.abs(new Date() - dataInstalacao);
                    diasEmCampo = Math.ceil(diffTime / (1000 * 60 * 60 * 24));

                    const previsaoRetirada = new Date(dataInstalacao);
                    previsaoRetirada.setDate(previsaoRetirada.getDate() + 7);
                    previsaoRetiradaFmt = previsaoRetirada.toLocaleDateString('pt-BR');
                }

                const fazendaNome = findShapefileProp(talhaoProps, ['NM_IMOVEL', 'NM_FAZENDA', 'NOME_FAZEN', 'FAZENDA']) || trap.fazendaNome || 'N/A';
                const fundoAgricola = findShapefileProp(talhaoProps, ['FUNDO_AGR']) || trap.fazendaCode || 'N/A';
                const talhaoNome = findShapefileProp(talhaoProps, ['CD_TALHAO', 'COD_TALHAO', 'TALHAO']) || trap.talhaoNome || 'N/A';


                return {
                    ...trap,
                    fazendaNome: fazendaNome,
                    fundoAgricola: fundoAgricola,
                    talhaoNome: talhaoNome,
                    dataInstalacaoFmt: dataInstalacao ? dataInstalacao.toLocaleDateString('pt-BR') : 'N/A',
                    previsaoRetiradaFmt: previsaoRetiradaFmt,
                    diasEmCampo: diasEmCampo,
                    instaladoPorNome: usersMap[trap.instaladoPor] || 'Desconhecido',
                };
            });

            if (fazendaCodigo) {
                const farmQuery = db.collection('fazendas').where('companyId', '==', companyId).where('code', '==', fazendaCodigo).limit(1);
                const farm = await farmQuery.get();
                if (!farm.empty) {
                    const farmName = farm.docs[0].data().name;
                    enrichedData = enrichedData.filter(d => d.fazendaNome === farmName);
                } else {
                    enrichedData = [];
                }
            }

            let currentY = await generatePdfHeader(doc, title, companyId);

            const headers = ['Fundo Agrícola', 'Fazenda', 'Talhão', 'Data Inst.', 'Previsão Retirada', 'Dias Campo', 'Instalado Por', 'Obs.'];
            const columnWidths = [90, 140, 80, 80, 80, 65, 90, 157];
            const rowHeight = 18;
            const textPadding = 5;

            currentY = drawRow(doc, headers, currentY, true, false, columnWidths, textPadding, rowHeight);

            for (const trap of enrichedData) {
                currentY = await checkPageBreak(doc, currentY, title);
                const rowData = [
                    trap.fundoAgricola,
                    trap.fazendaNome,
                    trap.talhaoNome,
                    trap.dataInstalacaoFmt,
                    trap.previsaoRetiradaFmt,
                    trap.diasEmCampo,
                    trap.instaladoPorNome,
                    trap.observacoes || ''
                ];
                currentY = drawRow(doc, rowData, currentY, false, false, columnWidths, textPadding, rowHeight);
            }

            generatePdfFooter(doc, generatedBy);
            doc.end();

        } catch (error) {
            console.error("Erro ao gerar PDF de Armadilhas Ativas:", error);
            if (!res.headersSent) res.status(500).send(`Erro ao gerar relatório: ${error.message}`);
            else doc.end();
        }
    });

    app.get('/reports/armadilhas-ativas/csv', async (req, res) => {
        try {
            const { inicio, fim, fazendaCodigo, companyId } = req.query;
            if (!companyId) {
                return res.status(400).send('O ID da empresa é obrigatório.');
            }
            let query = db.collection('armadilhas').where('companyId', '==', companyId).where('status', '==', 'Ativa');
            
            if (inicio) query = query.where('dataInstalacao', '>=', admin.firestore.Timestamp.fromDate(new Date(inicio + 'T00:00:00')));
            if (fim) query = query.where('dataInstalacao', '<=', admin.firestore.Timestamp.fromDate(new Date(fim + 'T23:59:59')));

            const snapshot = await query.get();
            let data = [];
            snapshot.forEach(doc => data.push({ id: doc.id, ...doc.data() }));

            if (data.length === 0) return res.status(404).send('Nenhum dado encontrado para os filtros selecionados.');

            const usersSnapshot = await db.collection('users').where('companyId', '==', companyId).get();
            const usersMap = {};
            usersSnapshot.forEach(doc => {
                usersMap[doc.id] = doc.data().username || doc.data().email;
            });

            const geojsonData = await getShapefileData(companyId);

            let enrichedData = data.map(trap => {
                const talhaoProps = findTalhaoForTrap(trap, geojsonData);
                const dataInstalacao = safeToDate(trap.dataInstalacao);
                let diasEmCampo = 'N/A';
                let previsaoRetiradaFmt = 'N/A';

                if (dataInstalacao) {
                    const diffTime = Math.abs(new Date() - dataInstalacao);
                    diasEmCampo = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                    const previsaoRetirada = new Date(dataInstalacao);
                    previsaoRetirada.setDate(previsaoRetirada.getDate() + 7);
                    previsaoRetiradaFmt = previsaoRetirada.toLocaleDateString('pt-BR');
                }

                return {
                    fundoAgricola: findShapefileProp(talhaoProps, ['FUNDO_AGR']) || trap.fundoAgricola || 'N/A',
                    fazendaNome: findShapefileProp(talhaoProps, ['NM_IMOVEL', 'NM_FAZENDA', 'NOME_FAZEN', 'FAZENDA']) || trap.fazendaNome || 'N/A',
                    talhaoNome: findShapefileProp(talhaoProps, ['CD_TALHAO', 'COD_TALHAO', 'TALHAO']) || trap.talhaoNome || 'N/A',
                    dataInstalacao: dataInstalacao ? dataInstalacao.toLocaleDateString('pt-BR') : 'N/A',
                    previsaoRetirada: previsaoRetiradaFmt,
                    diasEmCampo: diasEmCampo,
                    instaladoPor: usersMap[trap.instaladoPor] || 'Desconhecido',
                    observacoes: trap.observacoes || ''
                };
            });
            
            if (fazendaCodigo) {
                const farmQuery = db.collection('fazendas').where('companyId', '==', companyId).where('code', '==', fazendaCodigo).limit(1);
                const farm = await farmQuery.get();
                if (!farm.empty) {
                    const farmName = farm.docs[0].data().name;
                    enrichedData = enrichedData.filter(d => d.fazendaNome === farmName);
                } else {
                    enrichedData = [];
                }
            }

            const filePath = path.join(os.tmpdir(), `armadilhas_instaladas_report_${Date.now()}.csv`);
            const csvWriter = createObjectCsvWriter({
                path: filePath,
                header: [
                    { id: 'fundoAgricola', title: 'Fundo Agrícola' },
                    { id: 'fazendaNome', title: 'Fazenda' },
                    { id: 'talhaoNome', title: 'Talhão' },
                    { id: 'dataInstalacao', title: 'Data Instalação' },
                    { id: 'previsaoRetirada', title: 'Previsão Retirada' },
                    { id: 'diasEmCampo', title: 'Dias em Campo' },
                    { id: 'instaladoPor', title: 'Instalado Por' },
                    { id: 'observacoes', title: 'Observações' }
                ]
            });

            await csvWriter.writeRecords(enrichedData);
            res.download(filePath);

        } catch (error) {
            console.error("Erro ao gerar CSV de Armadilhas Ativas:", error);
            res.status(500).send('Erro ao gerar relatório.');
        }
    });

    const getRiskViewData = async (filters) => {
        const { companyId, inicio, fim, fazendaCodigo, riskOnly } = filters;
        if (!companyId) {
            throw new Error("O ID da empresa é obrigatório para calcular o risco.");
        }

        // 1. Buscar todas as coletas de armadilhas no período para identificar fazendas ativas.
        let collectedTrapsInRangeQuery = db.collection('armadilhas').where('companyId', '==', companyId).where('status', '==', 'Coletada');
        if (inicio) {
            collectedTrapsInRangeQuery = collectedTrapsInRangeQuery.where('dataColeta', '>=', new Date(inicio + 'T00:00:00Z'));
        }
        if (fim) {
            const endDate = new Date(fim);
            endDate.setUTCDate(endDate.getUTCDate() + 1);
            collectedTrapsInRangeQuery = collectedTrapsInRangeQuery.where('dataColeta', '<', endDate);
        }
        const collectedTrapsInRangeSnapshot = await collectedTrapsInRangeQuery.get();
        const collectedTrapsInRange = [];
        collectedTrapsInRangeSnapshot.forEach(doc => {
            collectedTrapsInRange.push({ id: doc.id, ...doc.data() });
        });

        // 2. Agrupar coletas por fazenda.
        const trapsByFarmCode = collectedTrapsInRange.reduce((acc, trap) => {
            const code = String(trap.fazendaCode || 'unknown').trim();
            if (!acc[code]) {
                acc[code] = [];
            }
            acc[code].push(trap);
            return acc;
        }, {});

        // Se um código de fazenda específico for fornecido, filtre para usar apenas essa fazenda.
        let activeFarmCodes = Object.keys(trapsByFarmCode);
        if (fazendaCodigo) {
            activeFarmCodes = activeFarmCodes.filter(code => code === String(fazendaCodigo).trim());
        }

        if (activeFarmCodes.length === 0) {
            return { reportFarms: [], farmRiskData: {}, latestCycleTraps: [] };
        }

        // 3. Buscar os dados de todas as fazendas ativas, lidando com o limite de 30 do 'IN'.
        const allActiveFarmsData = [];
        if (activeFarmCodes.length > 0) {
            const CHUNK_SIZE = 30; // Limite da consulta 'IN' do Firestore.
            const farmCodeChunks = [];
            for (let i = 0; i < activeFarmCodes.length; i += CHUNK_SIZE) {
                farmCodeChunks.push(activeFarmCodes.slice(i, i + CHUNK_SIZE));
            }

            const queryPromises = farmCodeChunks.map(chunk =>
                db.collection('fazendas')
                  .where('companyId', '==', companyId)
                  .where('code', 'in', chunk)
                  .get()
            );

            const snapshotResults = await Promise.all(queryPromises);
            snapshotResults.forEach(snapshot => {
                snapshot.forEach(doc => {
                    allActiveFarmsData.push({ id: doc.id, ...doc.data() });
                });
            });
        }

        const reportFarms = [];
        const farmRiskData = {};
        let latestCycleTraps = []; // Coletará as armadilhas do ciclo mais recente de todas as fazendas para o mapa.

        // 4. Calcular o risco para cada fazenda ativa, espelhando a lógica do frontend.
        for (const farm of allActiveFarmsData) {
            const farmCode = String(farm.code).trim();
            const collectedTrapsOnFarm = trapsByFarmCode[farmCode] || [];

            if (collectedTrapsOnFarm.length === 0) {
                farmRiskData[farm.code] = { riskPercentage: 0, totalTraps: 0, highCountTraps: 0 };
                reportFarms.push({ ...farm, riskPercentage: 0, totalTraps: 0, highCountTraps: 0 });
                continue;
            }

            // a. Encontrar a data de coleta mais recente na fazenda.
            let mostRecentCollectionDate = new Date(0);
            collectedTrapsOnFarm.forEach(trap => {
                const collectionDate = safeToDate(trap.dataColeta);
                if (collectionDate > mostRecentCollectionDate) {
                    mostRecentCollectionDate = collectionDate;
                }
            });

            // b. Filtrar para obter apenas as coletas desse dia específico.
            const latestCycleCollections = collectedTrapsOnFarm.filter(trap => {
                const collectionDate = safeToDate(trap.dataColeta);
                return collectionDate.getFullYear() === mostRecentCollectionDate.getFullYear() &&
                       collectionDate.getMonth() === mostRecentCollectionDate.getMonth() &&
                       collectionDate.getDate() === mostRecentCollectionDate.getDate();
            });

            // c. Deduplicar coletas para a mesma armadilha, mantendo a mais recente por timestamp.
            const latestUniqueCollections = new Map();
            latestCycleCollections.forEach(trap => {
                const trapKey = trap.id;
                const existing = latestUniqueCollections.get(trapKey);
                const collectionDate = safeToDate(trap.dataColeta);
                if (!existing || collectionDate > safeToDate(existing.dataColeta)) {
                    latestUniqueCollections.set(trapKey, trap);
                }
            });
            const finalCycleTraps = Array.from(latestUniqueCollections.values());
            latestCycleTraps.push(...finalCycleTraps); // Adiciona ao agregado para o mapa

            // d. Calcular o risco com base neste ciclo final.
            const highCountTraps = finalCycleTraps.filter(t => t.contagemMariposas >= 6);
            const divisor = finalCycleTraps.length;
            const riskPercentage = divisor > 0 ? (highCountTraps.length / divisor) * 100 : 0;

            farmRiskData[farm.code] = {
                riskPercentage,
                totalTraps: divisor,
                highCountTraps: highCountTraps.length
            };

            reportFarms.push({
                ...farm,
                riskPercentage: riskPercentage,
                totalTraps: divisor,
                highCountTraps: highCountTraps.length
            });
        }

        // 5. Ordenar e filtrar o resultado final.
        reportFarms.sort((a, b) => (parseInt(a.code, 10) || 0) - (parseInt(b.code, 10) || 0));

        let finalReportFarms = reportFarms;
        if (riskOnly === 'true') {
            finalReportFarms = reportFarms.filter(farm => farm.riskPercentage >= 30);
        }

        return { reportFarms: finalReportFarms, farmRiskData, latestCycleTraps };
    };

    app.get('/reports/risk-view/pdf', async (req, res) => {
        const doc = new PDFDocument({ margin: 30, size: 'A4', bufferPages: true, autoFirstPage: false });
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename=relatorio_risco.pdf`);
        doc.pipe(res);

        try {
            const { generatedBy, companyId } = req.query;
            if (!companyId) {
                throw new Error('O ID da empresa não foi fornecido.');
            }

            const { reportFarms, latestCycleTraps } = await getRiskViewData(req.query);
            const geojsonData = await getShapefileData(companyId);

            if (reportFarms.length === 0) {
                doc.addPage({ layout: 'portrait' }); // Adiciona a primeira página apenas se necessário
                await generatePdfHeader(doc, 'Relatório de Visualização de Risco', companyId);
                doc.text('Nenhuma fazenda com coletas encontrada para os filtros selecionados.');
                generatePdfFooter(doc, generatedBy);
                return doc.end();
            }

            let logoBase64 = null;
            try {
                const configDoc = await db.collection('config').doc(companyId).get();
                if (configDoc.exists && configDoc.data().logoBase64) {
                    logoBase64 = configDoc.data().logoBase64;
                }
            } catch (e) {
                console.error("Could not fetch company logo:", e);
            }


            for (const farm of reportFarms) {
                // PRIMEIRO, calcular todos os dados do talhão para que possam ser usados tanto no mapa quanto na tabela.
                const farmTraps = latestCycleTraps.filter(t => (t.fazendaCode ? String(t.fazendaCode).trim() === String(farm.code).trim() : t.fazendaNome === farm.name));
                const trapsByTalhao = {};
                if (geojsonData) {
                    for (const trap of farmTraps) {
                        const talhaoProps = findTalhaoForTrap(trap, geojsonData);
                        const talhaoNome = findShapefileProp(talhaoProps, ['CD_TALHAO', 'COD_TALHAO', 'TALHAO']) || trap.talhaoNome || 'N/A';
                        if (!trapsByTalhao[talhaoNome]) {
                            trapsByTalhao[talhaoNome] = { total: 0, high: 0, mothSum: 0 };
                        }
                        trapsByTalhao[talhaoNome].total++;
                        trapsByTalhao[talhaoNome].mothSum += trap.contagemMariposas || 0;
                        if (trap.contagemMariposas >= 6) {
                            trapsByTalhao[talhaoNome].high++;
                        }
                    }
                }

                doc.addPage({ layout: 'landscape', margin: 30 });
                const pageMargin = 30;

                // --- LEFT COLUMN: MAP ---
                const mapAreaWidth = doc.page.width * 0.60;
                const mapX = pageMargin;
                const mapY = pageMargin;
                const mapWidth = mapAreaWidth - pageMargin;
                const mapHeight = doc.page.height - (pageMargin * 2);

                if (geojsonData) {
                    const farmFeatures = geojsonData.features.filter(f => {
                        if (!f.properties) return false;
                        const propKeys = Object.keys(f.properties);
                        const codeKey = propKeys.find(k => k.toLowerCase() === 'fundo_agr');
                        if (!codeKey) return false;
                        const featureFarmCode = f.properties[codeKey];
                        return featureFarmCode && parseInt(featureFarmCode, 10) === parseInt(farm.code, 10);
                    });

                    if (farmFeatures.length > 0) {
                        const allCoords = farmFeatures.flatMap(f => f.geometry.type === 'Polygon' ? f.geometry.coordinates[0] : f.geometry.coordinates.flatMap(p => p[0]));
                        const bbox = {
                            minX: Math.min(...allCoords.map(c => c[0])), maxX: Math.max(...allCoords.map(c => c[0])),
                            minY: Math.min(...allCoords.map(c => c[1])), maxY: Math.max(...allCoords.map(c => c[1])),
                        };
                        const scaleX = mapWidth / (bbox.maxX - bbox.minX);
                        const scaleY = mapHeight / (bbox.maxY - bbox.minY);
                        const scale = Math.min(scaleX, scaleY) * 0.95;
                        const offsetX = mapX + (mapWidth - (bbox.maxX - bbox.minX) * scale) / 2;
                        const offsetY = mapY + (mapHeight - (bbox.maxY - bbox.minY) * scale) / 2;

                        const transformCoord = (coord) => [ (coord[0] - bbox.minX) * scale + offsetX, (bbox.maxY - coord[1]) * scale + offsetY ];

                        doc.save();
                        doc.lineWidth(0.5).strokeColor('#555');

                        farmFeatures.forEach(feature => {
                            const talhaoNome = findShapefileProp(feature.properties, ['CD_TALHAO', 'COD_TALHAO', 'TALHAO']) || 'N/A';
                            const talhaoInfo = trapsByTalhao[talhaoNome];
                            let fillColor = '#d3d3d3';

                            if (talhaoInfo) {
                                const riskPerc = talhaoInfo.total > 0 ? (talhaoInfo.high / talhaoInfo.total) * 100 : 0;
                                if (riskPerc >= 30) {
                                    fillColor = '#d9534f';
                                } else if (riskPerc > 0) {
                                    fillColor = '#f0ad4e';
                                } else {
                                    fillColor = '#5cb85c';
                                }
                            }

                            doc.fillColor(fillColor);
                            const polygons = feature.geometry.type === 'Polygon' ? [feature.geometry.coordinates] : feature.geometry.coordinates;
                            polygons.forEach(polygon => {
                                const path = polygon[0];
                                const firstPoint = transformCoord(path[0]);
                                doc.moveTo(firstPoint[0], firstPoint[1]);
                                for (let i = 1; i < path.length; i++) doc.lineTo(...transformCoord(path[i]));
                                doc.fillAndStroke();
                            });
                        });

                        farmTraps.forEach(trap => {
                            if (trap.longitude && trap.latitude) {
                                const [trapX, trapY] = transformCoord([trap.longitude, trap.latitude]);
                                const isHighRisk = trap.contagemMariposas >= 6;
                                const fillColor = isHighRisk ? '#d9534f' : '#5cb85c';
                                // Thicker stroke and slightly smaller radius to make the marker pop
                                doc.lineWidth(1).circle(trapX, trapY, 2.5).fillAndStroke(fillColor, '#000');
                            }
                        });
                        doc.restore();
                    } else {
                         doc.fontSize(10).text('Geometria da fazenda não encontrada no shapefile.', mapX + 10, mapY + 10);
                    }
                } else {
                     doc.fontSize(10).text('Shapefile não carregado. Mapa não pode ser gerado.', mapX + 10, mapY + 10);
                }

                // --- RIGHT COLUMN: DATA ---
                const dataX = mapAreaWidth + 15;
                const dataWidth = doc.page.width - mapAreaWidth - (pageMargin * 2) - 15;
                let currentY = pageMargin;

                doc.fontSize(16).font('Helvetica-Bold').text(`PROJETO - ${farm.code} - FAZ.`, dataX, currentY, { width: dataWidth, continued: true });
                doc.fontSize(16).font('Helvetica-Bold').text(farm.name.toUpperCase(), { width: dataWidth });
                currentY = doc.y + 2;
                doc.fontSize(10).font('Helvetica').text('Relatório de Risco de Armadilhas', dataX, currentY, { width: dataWidth });
                currentY = doc.y + 25;

                const summaryX = dataX;
                const summaryLabelWidth = 100;
                const summaryValueWidth = 50;

                const drawSummaryRow = (label, value, isBold = false) => {
                    const yPos = currentY;
                    doc.fontSize(10).font(isBold ? 'Helvetica-Bold' : 'Helvetica').text(label, summaryX, yPos, { width: summaryLabelWidth, align: 'left' });
                    doc.fontSize(10).font('Helvetica').text(value, summaryX + summaryLabelWidth, yPos, { width: summaryValueWidth, align: 'right' });
                    currentY = doc.y + 6;
                };

                drawSummaryRow('Total de Armadilhas:', farm.totalTraps);
                drawSummaryRow('Armadilhas em Alerta\n(>=6):', farm.highCountTraps);
                // Increased spacing to prevent overlap due to the two-line label above.
                doc.y += 8;
                currentY = doc.y;
                drawSummaryRow('Índice de Aplicação:', `${farm.riskPercentage.toFixed(2)}%`, true);

                currentY = doc.y + 25;

                doc.fontSize(12).font('Helvetica-Bold').text('Distribuição por Talhão', dataX, currentY, { width: dataWidth });
                currentY = doc.y + 8;

                const tableHeaderY = currentY;
                const tableCol1X = dataX;         // Talhão
                const tableCol2X = dataX + 80;    // Nº Arm.
                const tableCol3X = dataX + 125;   // >= 6
                const tableCol4X = dataX + 165;   // Mariposas
                const tableCol5X = dataX + 215;   // %

                doc.fontSize(10).font('Helvetica-Bold');
                doc.text('Talhão', tableCol1X, tableHeaderY, { width: 80, align: 'left' });
                doc.text('Nº Arm.', tableCol2X, tableHeaderY, { width: 45, align: 'center' });
                doc.text('>= 6', tableCol3X, tableHeaderY, { width: 40, align: 'center' });
                doc.text('Mariposas', tableCol4X, tableHeaderY, { width: 50, align: 'center' });
                doc.text('%', tableCol5X, tableHeaderY, { width: 40, align: 'center' });
                currentY = doc.y + 4;
                doc.lineWidth(1).moveTo(dataX, currentY).lineTo(dataX + dataWidth, currentY).strokeColor('#000').stroke();
                currentY += 8;

                const sortedTalhoes = Object.keys(trapsByTalhao).sort((a, b) => a.localeCompare(b, undefined, {numeric: true}));
                doc.fontSize(10).font('Helvetica');
                for(const talhao of sortedTalhoes) {
                    const info = trapsByTalhao[talhao];
                    const perc = info.total > 0 ? ((info.high / info.total) * 100).toFixed(1) : '0.0';

                    const yPos = currentY;
                    doc.text(talhao, tableCol1X, yPos, { width: 80, align: 'left' });
                    doc.text(info.total, tableCol2X, yPos, { width: 45, align: 'center' });
                    doc.text(info.high, tableCol3X, yPos, { width: 40, align: 'center' });
                    doc.text(info.mothSum, tableCol4X, yPos, { width: 50, align: 'center' });
                    doc.text(perc, tableCol5X, yPos, { width: 40, align: 'center' });

                    currentY = doc.y + 6;

                     if (currentY > doc.page.height - 80) {
                        doc.addPage({ layout: 'landscape', margin: 30 });
                        currentY = pageMargin;
                    }
                }

                if (logoBase64) {
                    const logoWidth = 70;
                    const logoX = dataX + (dataWidth / 2) - (logoWidth / 2);
                    const logoY = doc.page.height - pageMargin - 80;
                    doc.image(logoBase64, logoX, logoY, { width: logoWidth });
                }
            }

            // Move footer generation here to be outside the loop and after all pages are created.
            generatePdfFooter(doc, generatedBy);
            doc.end();

        } catch (error) {
            console.error("Erro ao gerar PDF de Visualização de Risco:", error);
            // If an error occurs, generate a simple PDF with the error message.
            if (!res.headersSent) {
                const errorDoc = new PDFDocument();
                res.setHeader('Content-Type', 'application/pdf');
                res.setHeader('Content-Disposition', 'attachment; filename=error.pdf');
                errorDoc.pipe(res);
                errorDoc.text('Ocorreu um erro ao gerar o relatório em PDF:');
                errorDoc.text(error.message);
                errorDoc.end();
            } else {
                 doc.end(); // Ensure the stream is closed if headers were already sent
            }
        }
    });

    app.get('/reports/risk-view/csv', async (req, res) => {
        try {
            const { companyId } = req.query;
            if (!companyId) {
                return res.status(400).send('O ID da empresa é obrigatório.');
            }

            const { reportFarms } = await getRiskViewData(req.query);

            if (reportFarms.length === 0) {
                return res.status(404).send('Nenhuma fazenda com coletas encontrada para os filtros selecionados.');
            }

            const filePath = path.join(os.tmpdir(), `relatorio_risco_${Date.now()}.csv`);
            const csvWriter = createObjectCsvWriter({
                path: filePath,
                header: [
                    { id: 'code', title: 'Código Fazenda' },
                    { id: 'name', title: 'Nome Fazenda' },
                    { id: 'totalTraps', title: 'Nº Armadilhas' },
                    { id: 'highCountTraps', title: 'Armadilhas >= 6' },
                    { id: 'riskPercentage', title: 'Índice de Aplicação (%)' }
                ]
            });

            const records = reportFarms.map(farm => ({
                code: farm.code,
                name: farm.name,
                totalTraps: farm.totalTraps,
                highCountTraps: farm.highCountTraps,
                riskPercentage: farm.riskPercentage.toFixed(2)
            }));

            records.sort((a, b) => a.code - b.code);

            await csvWriter.writeRecords(records);
            res.download(filePath);

        } catch (error) {
            console.error("Erro ao gerar CSV de Visualização de Risco:", error);
            res.status(500).send('Erro ao gerar relatório.');
        }
    });

} catch (error) {
    console.error("ERRO CRÍTICO AO INICIALIZAR FIREBASE:", error);
    app.use((req, res) => res.status(500).send('Erro de configuração do servidor.'));
}

app.listen(port, () => {
    console.log(`Servidor de relatórios rodando na porta ${port}`);
});