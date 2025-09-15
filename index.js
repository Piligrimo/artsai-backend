import express from 'express'
import Papa from 'papaparse'
import fs from 'fs'
import cors from 'cors'

let impressions = new Map()
const graphData = []
const tableBySite = new Map()
const tableByDMA = new Map()
// minDate: 1626881147000, maxDate: 1628462885000 
const minDate = 1626881147000
const maxDate = 1628117285000


const hours24 = 1000 * 60 * 60 * 24

const numberOfDays = (maxDate - minDate) / hours24

let isInitialized = false

for (let index = 0; index < numberOfDays; index++) {
    graphData[index] = {
        impression_count: 0,
        reg_time: minDate + hours24 * index,
        fclick: 0,
        registration: 0,
        content: 0,
        lead: 0,
        signup: 0,
        misc: 0
    }
}

const readXFile = () => { 
    Papa.parse(fs.createReadStream("./interview.X.csv"), {
        complete: function(results) {
            const headers = results.data.splice(0, 1)[0]
            results.data.forEach((item) => {
                const result = {}
                headers.forEach((value, i) => {
                    result[value] = item[i]
                }) 
                
                impressions.set(result.uid, result)
                const stepIndex = Math.floor((new Date(result.reg_time) - minDate) / hours24)
                if (stepIndex < numberOfDays)
                    graphData[stepIndex].impression_count ++
                const siteRow = tableBySite.get(result.site_id) 
                if (!siteRow) {
                    tableBySite.set(result.site_id, {
                        impression_count: 1,
                        fclick: 0,
                        registration: 0,
                        content: 0,
                        lead: 0,
                        signup: 0,
                        misc: 0
                    })
                } else {
                    siteRow.impression_count++
                }
                const dmaRow = tableByDMA.get(result.mm_dma) 
                if (!dmaRow) {
                    tableByDMA.set(result.mm_dma, {
                        impression_count: 1,
                        fclick: 0,
                        registration: 0,
                        content: 0,
                        lead: 0,
                        signup: 0,
                        misc: 0
                    })
                } else {
                    dmaRow.impression_count++
                }
            })
            readYFile()
        }
    });
}
const readYFile = () => {
    Papa.parse(fs.createReadStream("./interview.y.csv"), {
        complete: function(results) {
            
            const headers = results.data.splice(0, 1)[0]
            results.data.forEach((item) => {
                
                const result = {}
                headers.forEach((value, i) => {
                    result[value] = item[i]
                }) 
                const tag = result.tag[0] === 'v' ? result.tag.substring(1) : result.tag    
                const impression = impressions.get(result.uid)
                if (!impression) return

                const siteRow = tableBySite.get(impression.site_id) 
                const dmaRow = tableByDMA.get(impression.mm_dma) 
                 
                const stepIndex = Math.floor((new Date(impression.reg_time) - minDate) / hours24)
                if (stepIndex < numberOfDays) {
                    graphData[stepIndex][tag]++
                    if (siteRow) siteRow[tag]++
                    if (dmaRow) dmaRow[tag]++
                }
            })
            isInitialized = true
            console.log('Initialized');
            
        }
    });
}



const app = express()
app.use(express.json())
app.use(cors())

const port = 8080
readXFile()


app.get('/', (req, res) => {
    try {
        if (isInitialized) {
            const data = graphData.map(item => {
                return {
                    date: item.reg_time,
                    value: (item.fclick / item.impression_count) * 100
                }
            })
            res.send(data)
        }
        else
            res.status(503).send('Данные еще не загрузились, попробуйте еще раз позже')
    } catch (e) {
        console.error(e);   
    }
})

app.get('/ctr', (req, res) => {
    try {
        if (isInitialized)
            {
            const data = graphData.map(item => {
                return {
                    date: item.reg_time,
                    value: (item.fclick / item.impression_count) * 100
                }
            })
            res.send(data)
        }
        else
            res.status(503).send('Данные еще не загрузились, попробуйте еще раз позже')
    } catch (e) {
        console.error(e);   
    }
})

app.get('/evpm', (req, res) => {
    const tag = req.query.event
    try {
        if (isInitialized)
            {
            const data = graphData.map(item => {
                if (!Object.hasOwn(item, tag)) {
                    res.status(400).send('Некорректное событие!')
                    return
                }
                return {
                    date: item.reg_time,
                    value: (item[tag] / item.impression_count) * 1000
                }
            })
            res.send(data)
        }
        else
            res.status(503).send('Данные еще не загрузились, попробуйте еще раз позже')
    } catch (e) {
        console.error(e);   
    }
})


app.get('/table', (req, res) => {
    const {event = 'registration', type = 'site', page = 0, pageSize = 20} = req.query
    try {
        if (isInitialized) {

            const rawData = type === 'site' ? tableBySite : tableByDMA
            const data = [...rawData].slice(page * pageSize, (Number(page)+1) * pageSize).map(([site_id, item]) => {
                return {
                    site_id,
                    impression_count: item.impression_count,
                    ctr: ((item.fclick / item.impression_count) * 100).toPrecision(3),
                    evpm: ((item[event] / item.impression_count) * 1000).toPrecision(3)
                }
            })
            res.send({
                data,
                total: rawData.size
            })
        }
        else
            res.status(503).send('Данные еще не загрузились, попробуйте еще раз позже')
    } catch (e) {
        console.error(e);   
    }
})

app.listen(port, () => {
   console.log(`Running on http://localhost:${port}/`)
})