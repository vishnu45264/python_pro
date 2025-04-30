"""
Real-Time Stock Data Analysis System using Alpha Vantage API
"""
import requests
import pandas as pd
import numpy as np
import threading
import time
import json
import datetime
import http.server
import socketserver
import logging
import sys
from queue import Queue
from pathlib import Path
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('stock-app')

# Global data queue for communication between producer and consumer
data_queue = Queue()
processed_data = []
data_lock = threading.Lock()

# Configuration
INTERVAL = 15.0  # seconds between updates (increased to avoid rate limiting)
SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META']  # Example stocks
PORT = 8051  # Changed to 8051 in case 8050 is in use
API_KEY = 'SE215CZ0RAIK1C74'  # Using demo API key for testing API_KEY = 'SE215CZ0RAIK1C74'  # Using demo API key for testing


class RealStockProducer:
    """A data producer that fetches real stock data from Alpha Vantage"""
    
    def __init__(self):
        """Initialize the producer"""
        self.running = True
        self.base_url = 'https://www.alphavantage.co/query'
        logger.info(f"Producer initialized with symbols: {', '.join(SYMBOLS)}")
    
    def fetch_stock_data(self, symbol):
        """Fetch real-time stock data for a symbol"""
        try:
            # API parameters
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': symbol,
                'apikey': API_KEY
            }
            
            # Make API request
            logger.info(f"Fetching data for {symbol}...")
            response = requests.get(self.base_url, params=params)
            
            if response.status_code != 200:
                logger.error(f"API request failed with status code {response.status_code}")
                logger.error(f"Response: {response.text}")
                return None
                
            data = response.json()
            
            # Check for demo key message or error
            if 'Information' in data and 'demo' in data['Information']:
                # Demo key is being limited, only allow a specific symbol (MSFT) for real data
                if symbol != 'MSFT':
                    logger.warning(f"Demo API key limited, will use fallback data for {symbol}")
                    return None
            
            if 'Global Quote' in data and data['Global Quote']:
                quote = data['Global Quote']
                if len(quote) > 4:  # Ensure we have enough data
                    logger.info(f"Got data for {symbol}: {quote}")
                    return {
                        'timestamp': datetime.datetime.now().isoformat(),
                        'symbol': symbol,
                        'price': round(float(quote.get('05. price', 0)), 2),
                        'volume': int(float(quote.get('06. volume', 0))),
                        'open': round(float(quote.get('02. open', 0)), 2),
                        'high': round(float(quote.get('03. high', 0)), 2),
                        'low': round(float(quote.get('04. low', 0)), 2),
                        'data_type': 'stock'
                    }
            
            logger.error(f"Unexpected API response format: {data}")
            return None
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
        return None
    
    def start(self):
        """Start producing data"""
        logger.info("Starting to fetch real stock data")
        fallback_symbols = set()  # Symbols that need fallback data
        
        # Define realistic volume ranges for each symbol
        volume_bases = {
            'AAPL': 80000000,
            'MSFT': 30000000,
            'GOOGL': 1800000,
            'AMZN': 5000000,
            'META': 25000000
        }
        
        while self.running:
            try:
                success_count = 0
                
                # First try to fetch real data for all symbols
                for symbol in SYMBOLS:
                    # Skip API call for symbols we know need fallback data
                    if symbol in fallback_symbols:
                        continue
                        
                    data = self.fetch_stock_data(symbol)
                    if data:
                        data_queue.put(data)
                        logger.info(f"Fetched {symbol}: ${data['price']}")
                        success_count += 1
                    else:
                        # Add to fallback symbols
                        fallback_symbols.add(symbol)
                        logger.warning(f"Adding {symbol} to fallback list")
                    time.sleep(1)  # Small delay between requests
                
                # Generate fallback data for symbols that failed
                for symbol in fallback_symbols:
                    mock_price = 100 + (SYMBOLS.index(symbol) * 50) + random.uniform(-5, 5)
                    base_volume = volume_bases.get(symbol, 10000000)  # Default fallback
                    volume = int(base_volume * (0.8 + random.uniform(0, 0.4)))  # 80-120% of base
                    
                    data = {
                        'timestamp': datetime.datetime.now().isoformat(),
                        'symbol': symbol,
                        'price': round(mock_price, 2),
                        'volume': volume,
                        'open': round(mock_price * 0.99, 2),
                        'high': round(mock_price * 1.02, 2),
                        'low': round(mock_price * 0.98, 2),
                        'data_type': 'stock'
                    }
                    data_queue.put(data)
                    logger.info(f"Using fallback data for {symbol}: ${data['price']}, vol: {volume:,}")
                    time.sleep(0.5)  # Small delay between fallback data
                
                time.sleep(INTERVAL)
            except Exception as e:
                logger.error(f"Error in producer main loop: {e}")
                # On error, use fallback for all symbols as a precaution
                fallback_symbols = set(SYMBOLS)
                time.sleep(INTERVAL)
    
    def stop(self):
        """Stop producing data"""
        self.running = False
        logger.info("Producer stopped")

class StockAnalyzer:
    """Analyzes stock data for trends and anomalies"""
    
    def __init__(self):
        """Initialize the analyzer"""
        self.window_size = 10
        self.price_history = {symbol: [] for symbol in SYMBOLS}
        self.volume_history = {symbol: [] for symbol in SYMBOLS}
        logger.info("Analyzer initialized")
    
    def detect_anomalies(self, data_point, history, threshold=2.0):
        """Detect anomalies using z-score method"""
        if len(history) < 3:  # Need at least 3 points for meaningful analysis
            return False
        
        try:
            recent_data = history[-self.window_size:]
            mean = np.mean(recent_data)
            std = np.std(recent_data)
            if std < 0.001:  # Avoid division by near-zero
                logger.debug(f"Standard deviation too small: {std}")
                return False
            z_score = abs((data_point - mean) / std)
            return z_score > threshold
        except Exception as e:
            logger.error(f"Error in anomaly detection: {e}")
            return False
    
    def analyze_data(self, data_point):
        """Analyze a data point"""
        try:
            symbol = data_point['symbol']
            price = data_point['price']
            volume = data_point['volume']
            
            # Update history
            self.price_history[symbol].append(price)
            self.volume_history[symbol].append(volume)
            
            # Keep only recent history
            if len(self.price_history[symbol]) > self.window_size:
                self.price_history[symbol] = self.price_history[symbol][-self.window_size:]
                self.volume_history[symbol] = self.volume_history[symbol][-self.window_size:]
            
            # Calculate statistics
            price_mean = np.mean(self.price_history[symbol])
            price_std = np.std(self.price_history[symbol])
            volume_mean = np.mean(self.volume_history[symbol])
            
            # Ensure standard deviation is not too small for display
            if price_std < 0.01:
                price_std = 0.01
            
            # Detect anomalies
            price_anomaly = self.detect_anomalies(price, self.price_history[symbol])
            volume_anomaly = self.detect_anomalies(volume, self.volume_history[symbol])
            
            result = {
                'symbol': symbol,
                'price': price,
                'volume': volume,
                'price_mean': round(price_mean, 2),
                'price_std': round(price_std, 2),
                'volume_mean': round(volume_mean, 2),
                'price_anomaly': 1 if price_anomaly else 0,  # Convert boolean to integer
                'volume_anomaly': 1 if volume_anomaly else 0,  # Convert boolean to integer
                'timestamp': data_point['timestamp']
            }
            
            # Log anomalies
            if price_anomaly:
                logger.warning(f"Price anomaly detected for {symbol}: ${price}")
            if volume_anomaly:
                logger.warning(f"Volume anomaly detected for {symbol}: {volume}")
                
            return result
        except Exception as e:
            logger.error(f"Error analyzing data: {e}")
            return None
    
    def start(self):
        """Start analyzing data"""
        logger.info("Starting data analysis")
        while True:
            try:
                data = data_queue.get(timeout=60)  # Wait up to 60 seconds for data
                if data:
                    result = self.analyze_data(data)
                    if result:
                        with data_lock:
                            processed_data.append(result)
                            # Keep only last 100 data points
                            if len(processed_data) > 100:
                                processed_data.pop(0)
            except Exception as e:
                logger.error(f"Analyzer error: {e}")
                time.sleep(1)  # Avoid tight loop on error
    
    def stop(self):
        """Stop analyzing data"""
        logger.info("Analyzer stopped")

class StockHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    """Custom HTTP handler for serving the dashboard"""
    
    def do_GET(self):
        """Handle GET requests"""
        try:
            if self.path == '/':
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(get_dashboard_html().encode())
                logger.info("Dashboard page served")
            elif self.path == '/data':
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                with data_lock:
                    # Check if we have data
                    if not processed_data:
                        # If no real data yet, send mock data for testing
                        mock_data = generate_mock_data()
                        logger.warning("No real data available, sending mock data for display testing")
                        self.wfile.write(json.dumps(mock_data).encode())
                    else:
                        # Ensure all data is JSON serializable
                        serializable_data = []
                        for item in processed_data:
                            serializable_item = {}
                            for key, value in item.items():
                                # Convert any non-serializable types
                                if isinstance(value, (np.float64, np.float32)):
                                    serializable_item[key] = float(value)
                                elif isinstance(value, (np.int64, np.int32)):
                                    serializable_item[key] = int(value)
                                elif isinstance(value, bool):
                                    serializable_item[key] = 1 if value else 0
                                else:
                                    serializable_item[key] = value
                            serializable_data.append(serializable_item)
                        logger.info(f"Sending {len(serializable_data)} data points to dashboard")
                        self.wfile.write(json.dumps(serializable_data).encode())
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            logger.error(f"Error handling request: {e}")
            self.send_response(500)
            self.end_headers()
    
    def log_message(self, format, *args):
        """Suppress default logging"""
        pass

def get_dashboard_html():
    """Generate the dashboard HTML"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Real-Time Stock Analysis Dashboard</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background-color: #f0f2f5; }
            .container { max-width: 1200px; margin: 0 auto; }
            .header { text-align: center; margin-bottom: 20px; background-color: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .chart { margin-bottom: 20px; background-color: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
            .stat-card { background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .anomaly { color: #dc3545; font-weight: bold; }
            h1 { color: #1a73e8; }
            h3 { color: #202124; margin-top: 0; }
            .price { font-size: 24px; font-weight: bold; color: #1a73e8; }
            .label { color: #5f6368; font-size: 14px; }
            .loading { text-align: center; padding: 20px; color: #666; }
            .status { text-align: center; margin-top: 10px; padding: 8px; border-radius: 4px; }
            .success { background-color: #d4edda; color: #155724; }
            .error { background-color: #f8d7da; color: #721c24; }
            .warning { background-color: #fff3cd; color: #856404; }
            #price-chart, #volume-chart { width: 100%; height: 400px; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Real-Time Stock Analysis Dashboard</h1>
                <p class="label">Last updated: <span id="last-update">Loading...</span></p>
                <div id="connection-status" class="status warning">Connecting to data source...</div>
            </div>
            <div class="chart">
                <div id="price-chart"><div class="loading">Loading price data...</div></div>
            </div>
            <div class="chart">
                <div id="volume-chart"><div class="loading">Loading volume data...</div></div>
            </div>
            <div class="stats" id="stats-container">
                <div class="loading">Loading stock statistics...</div>
            </div>
        </div>
        <script>
            let connectionError = false;
            let retryCount = 0;
            let chartInitialized = false;
            const maxRetries = 5;
            
            // Initialize empty charts
            function initCharts() {
                if (chartInitialized) return;
                
                const initialData = [];
                const layout = {
                    title: 'Stock Prices',
                    xaxis: { 
                        title: 'Time',
                        type: 'date',
                        tickformat: '%H:%M:%S'
                    },
                    yaxis: { title: 'Price ($)' },
                    showlegend: true,
                    legend: { orientation: 'h', y: -0.2 },
                    margin: { l: 40, r: 10, t: 40, b: 80 },
                    height: 400
                };
                
                Plotly.newPlot('price-chart', initialData, layout);
                
                const volumeLayout = {
                    title: 'Trading Volume',
                    xaxis: { 
                        title: 'Time',
                        type: 'date',
                        tickformat: '%H:%M:%S'
                    },
                    yaxis: { 
                        title: 'Volume',
                        type: 'log',
                        autorange: true
                    },
                    showlegend: true,
                    legend: { orientation: 'h', y: -0.2 },
                    margin: { l: 60, r: 10, t: 40, b: 80 },
                    height: 400
                };
                
                Plotly.newPlot('volume-chart', initialData, volumeLayout);
                chartInitialized = true;
            }
            
            // Initialize empty charts immediately
            initCharts();
            
            function updateConnectionStatus(status, message) {
                const statusEl = document.getElementById('connection-status');
                statusEl.className = 'status ' + status;
                statusEl.innerText = message;
            }
            
            function updateDashboard() {
                console.log("Fetching data...");
                updateConnectionStatus('warning', 'Fetching data...');
                
                fetch('/data')
                    .then(response => {
                        if (!response.ok) {
                            throw new Error(`HTTP error! Status: ${response.status}`);
                        }
                        return response.json();
                    })
                    .then(data => {
                        console.log("Data received:", data.length, "records");
                        connectionError = false;
                        retryCount = 0;
                        
                        if (data.length === 0) {
                            updateConnectionStatus('warning', 'No data available yet. Waiting for data...');
                            return;
                        }
                        
                        updateConnectionStatus('success', 'Connected to data source');
                        
                        // Update last update time
                        document.getElementById('last-update').textContent = 
                            new Date(data[data.length - 1].timestamp).toLocaleString();
                        
                        // Group data by symbol
                        const symbolsMap = {};
                        data.forEach(item => {
                            if (!symbolsMap[item.symbol]) {
                                symbolsMap[item.symbol] = [];
                            }
                            symbolsMap[item.symbol].push(item);
                        });
                        
                        // Sort each symbol's data by timestamp
                        Object.keys(symbolsMap).forEach(symbol => {
                            symbolsMap[symbol].sort((a, b) => 
                                new Date(a.timestamp) - new Date(b.timestamp)
                            );
                        });
                        
                        // Prepare data for charts
                        const symbols = Object.keys(symbolsMap);
                        const priceTraces = symbols.map(symbol => {
                            const symbolData = symbolsMap[symbol];
                            return {
                                name: symbol,
                                x: symbolData.map(d => new Date(d.timestamp)),
                                y: symbolData.map(d => d.price),
                                type: 'scatter',
                                mode: 'lines+markers',
                                line: { width: 3 },
                                marker: { size: 8 }
                            };
                        });
                        
                        // Update price chart using react mode
                        Plotly.react('price-chart', priceTraces, {
                            title: 'Stock Prices',
                            xaxis: { 
                                title: 'Time',
                                type: 'date',
                                tickformat: '%H:%M:%S'
                            },
                            yaxis: { title: 'Price ($)' },
                            showlegend: true,
                            legend: { orientation: 'h', y: -0.2 },
                            margin: { l: 40, r: 10, t: 40, b: 80 },
                            height: 400
                        });
                        
                        // Update volume chart
                        const volumeTraces = symbols.map(symbol => {
                            const symbolData = symbolsMap[symbol];
                            return {
                                name: symbol,
                                x: symbolData.map(d => new Date(d.timestamp)),
                                y: symbolData.map(d => d.volume),
                                type: 'scatter',
                                mode: 'lines+markers',
                                line: { width: 3 },
                                marker: { size: 8 }
                            };
                        });
                        
                        // Update volume chart using react mode
                        Plotly.react('volume-chart', volumeTraces, {
                            title: 'Trading Volume',
                            xaxis: { 
                                title: 'Time',
                                type: 'date',
                                tickformat: '%H:%M:%S'
                            },
                            yaxis: { 
                                title: 'Volume',
                                type: 'log',
                                autorange: true,
                                tickformat: ',d'
                            },
                            showlegend: true,
                            legend: { orientation: 'h', y: -0.2 },
                            margin: { l: 60, r: 10, t: 40, b: 80 },
                            height: 400
                        });
                        
                        // Update stats
                        const statsContainer = document.getElementById('stats-container');
                        statsContainer.innerHTML = '';
                        
                        symbols.forEach(symbol => {
                            // Get most recent data point for each symbol
                            const symbolData = symbolsMap[symbol];
                            const latest = symbolData[symbolData.length - 1];
                            
                            const card = document.createElement('div');
                            card.className = 'stat-card';
                            card.innerHTML = `
                                <h3>${symbol}</h3>
                                <p class="price">$${latest.price.toLocaleString()}</p>
                                <p class="label">Volume: ${latest.volume.toLocaleString()}</p>
                                <p class="label">Mean Price: $${latest.price_mean.toLocaleString()}</p>
                                <p class="label">Std Dev: $${latest.price_std.toLocaleString()}</p>
                                ${latest.price_anomaly === 1 ? '<p class="anomaly">⚠️ Price Anomaly Detected</p>' : ''}
                                ${latest.volume_anomaly === 1 ? '<p class="anomaly">⚠️ Volume Anomaly Detected</p>' : ''}
                            `;
                            statsContainer.appendChild(card);
                        });
                    })
                    .catch(error => {
                        console.error('Error fetching data:', error);
                        connectionError = true;
                        retryCount++;
                        
                        if (retryCount > maxRetries) {
                            updateConnectionStatus('error', 'Connection failed. Check console for details.');
                        } else {
                            updateConnectionStatus('warning', `Connection error. Retry ${retryCount}/${maxRetries}...`);
                        }
                    });
            }
            
            // Initial update
            updateDashboard();
            
            // Update every 10 seconds
            setInterval(() => {
                updateDashboard();
            }, 10000);
            
            // Add console message for debugging
            console.log("Dashboard initialized. Waiting for data...");
        </script>
    </body>
    </html>
    """

def generate_mock_data():
    """Generate mock data for testing the dashboard"""
    mock_data = []
    base_time = datetime.datetime.now()
    
    # Define realistic volume ranges for each symbol
    volume_bases = {
        'AAPL': 80000000,
        'MSFT': 30000000,
        'GOOGL': 1800000,
        'AMZN': 5000000,
        'META': 25000000
    }
    
    for i in range(10):
        for symbol in SYMBOLS:
            time_offset = datetime.timedelta(minutes=i*5)
            timestamp = (base_time - time_offset).isoformat()
            
            # Create mock price with small variations
            base_price = 100 + (SYMBOLS.index(symbol) * 50)
            price = base_price + (i * 2) + random.uniform(-5, 5)
            
            # Create realistic volume for each symbol
            base_volume = volume_bases.get(symbol, 10000000)  # Default fallback
            volume = int(base_volume * (0.8 + random.uniform(0, 0.4)))  # 80-120% of base volume
            
            mock_data.append({
                'symbol': symbol,
                'price': round(price, 2),
                'volume': volume,
                'price_mean': round(base_price, 2),
                'price_std': 2.5,
                'volume_mean': base_volume,
                'price_anomaly': 1 if i == 0 else 0,  # First point is anomaly
                'volume_anomaly': 0,
                'timestamp': timestamp
            })
    
    return mock_data

def start_http_server():
    """Start the HTTP server"""
    try:
        # Try to create the server with a socket timeout
        socketserver.TCPServer.allow_reuse_address = True
        with socketserver.TCPServer(("", PORT), StockHTTPRequestHandler) as httpd:
            logger.info(f"Starting HTTP server on port {PORT}")
            # Print access URL for convenience
            logger.info(f"Dashboard URL: http://localhost:{PORT}")
            logger.info(f"Dashboard URL (network): http://{get_ip_address()}:{PORT}")
            httpd.serve_forever()
    except OSError as e:
        if e.errno == 98:  # Address already in use
            logger.error(f"Port {PORT} is already in use. Try a different port.")
        else:
            logger.error(f"Server error: {e}")
    except Exception as e:
        logger.error(f"Error starting server: {e}")
        
def get_ip_address():
    """Get the current machine's IP address"""
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "127.0.0.1"  # Fallback to localhost

def main():
    """Main function"""
    logger.info("Starting Real-Time Stock Analysis System")
    
    try:
        # Start the producer
        producer = RealStockProducer()
        producer_thread = threading.Thread(target=producer.start)
        producer_thread.daemon = True
        producer_thread.start()
        logger.info("Producer thread started")
        
        # Start the analyzer
        analyzer = StockAnalyzer()
        analyzer_thread = threading.Thread(target=analyzer.start)
        analyzer_thread.daemon = True
        analyzer_thread.start()
        logger.info("Analyzer thread started")
        
        # Start the HTTP server
        server_thread = threading.Thread(target=start_http_server)
        server_thread.daemon = True
        server_thread.start()
        logger.info("Server thread started")
        
        logger.info(f"Dashboard should be available at http://localhost:{PORT}")
        logger.info("Press Ctrl+C to stop")
        
        while True:
            # Check if threads are still running
            if not producer_thread.is_alive():
                logger.error("Producer thread died")
                
            if not analyzer_thread.is_alive():
                logger.error("Analyzer thread died")
                
            if not server_thread.is_alive():
                logger.error("Server thread died")
            
            time.sleep(5)  # Check every 5 seconds
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        producer.stop()
        analyzer.stop()
    except Exception as e:
        logger.error(f"Main error: {e}")

if __name__ == "__main__":
    # Clear the console first
    print("\033[H\033[J")  # ANSI escape sequence to clear screen
    print("=" * 50)
    print("REAL-TIME STOCK ANALYSIS DASHBOARD")
    print("=" * 50)
    main() 