import express from 'express'
import Papa from 'papaparse'
import fs from 'fs'


let impressions = new Map()
const graphData = []
// minDate: 1626881147000, maxDate: 1628462885000 
const minDate = 1626881147000
const maxDate = 1628462885000

const hours24 = 1000 * 60 * 60 * 24

const numberOfDays = (maxDate - minDate) / hours24

let isInitialized = false

console.log(numberOfDays);


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
                if (graphData[stepIndex]) {
                    graphData[stepIndex].impression_count ++
                } else {
                    graphData[stepIndex] = {
                        impression_count: 1,
                        reg_time: result.reg_time,
                        fclick: 0,
                        registration: 0,
                        content: 0,
                        lead: 0,
                        signup: 0,
                        misc: 0
                    }
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
                console.log(impression);
                if (!impression) return
                const stepIndex = Math.floor((new Date(impression.reg_time) - minDate) / hours24)
                graphData[stepIndex][tag]++
            })
            isInitialized = true
        }
    });
}



const app = express()
app.use(express.json())

const port = 3000
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

app.get('/crt', (req, res) => {
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

app.get('/evmp', (req, res) => {
    const tag = req.params.tag
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
                    value: (item[tag] / item.impression_count) * 100
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

app.listen(port, () => {
   console.log(`Running on http://localhost:${port}/`)
})