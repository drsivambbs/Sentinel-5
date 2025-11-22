// Dashboard JavaScript
document.addEventListener('DOMContentLoaded', function() {
    const sidebarItems = document.querySelectorAll('.sidebar-item[data-page]');
    const contentPages = document.querySelectorAll('.content-page');
    const pageTitle = document.getElementById('pageTitle');

    // Page titles mapping
    const pageTitles = {
        'dashboard': 'Dashboard',
        'cluster': 'Cluster Management'
    };

    // Handle sidebar navigation
    sidebarItems.forEach(item => {
        item.addEventListener('click', function(e) {
            e.preventDefault();
            
            const targetPage = this.getAttribute('data-page');
            
            // Remove active class from all items
            sidebarItems.forEach(i => i.classList.remove('active'));
            
            // Add active class to clicked item
            this.classList.add('active');
            
            // Hide all content pages
            contentPages.forEach(page => page.classList.add('hidden'));
            
            // Show target content page
            const targetContent = document.getElementById(targetPage + '-content');
            if (targetContent) {
                targetContent.classList.remove('hidden');
                targetContent.classList.add('animate-fade-in');
                

            }
            
            // Update page title
            if (pageTitles[targetPage]) {
                pageTitle.textContent = pageTitles[targetPage];
            }
        });
    });

    // Mobile sidebar toggle (if needed)
    function toggleSidebar() {
        const sidebar = document.getElementById('sidebar');
        sidebar.classList.toggle('open');
    }

    // Add mobile menu button if needed
    if (window.innerWidth <= 768) {
        // Mobile responsive behavior can be added here
    }

    // Load dashboard data on page load
    if (document.getElementById('dashboard-content')) {
        loadDashboardStats();
    }
});

// Load dashboard statistics
async function loadDashboardStats() {
    try {
        const API_URL = 'https://sentinel-dashboard-api-196547645490.asia-south1.run.app/api/dashboard/stats';
        
        console.log('Fetching data from:', API_URL);
        const response = await fetch(API_URL);
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const result = await response.json();
        console.log('API Response:', result);
        
        if (result.success && result.data) {
            updateDashboardCards(result.data);
        } else {
            console.error('API returned unsuccessful response:', result);
            showMockData();
        }
    } catch (error) {
        console.error('Error loading dashboard stats:', error);
        // Show mock data for development
        showMockData();
    }
}

// Update dashboard cards with real data
function updateDashboardCards(data) {
    console.log('Updating cards with data:', data);
    
    document.getElementById('loading-cards').classList.add('hidden');
    document.getElementById('stats-cards').classList.remove('hidden');
    
    // Update each card with proper null checking
    const totalCases = data.total_cases || 0;
    const geocodedPercent = data.geocoded_percentage || 0;
    const urbanCases = data.urban_cases || 0;
    const ruralCases = data.rural_cases || 0;
    
    document.getElementById('total-cases').textContent = totalCases.toLocaleString();
    document.getElementById('geocoded-percent').textContent = geocodedPercent + '%';
    document.getElementById('urban-cases').textContent = urbanCases.toLocaleString();
    document.getElementById('rural-cases').textContent = ruralCases.toLocaleString();
    
    // Update streaming status
    const streamingElement = document.getElementById('streaming-status');
    if (data.streaming_status && data.streaming_status.has_buffer) {
        streamingElement.textContent = 'Active';
        streamingElement.className = 'text-lg font-bold text-yellow-600';
    } else {
        streamingElement.textContent = 'Clear';
        streamingElement.className = 'text-lg font-bold text-green-600';
    }
}

// Show mock data for development
function showMockData() {
    const mockData = {
        total_cases: 1234,
        geocoded_percentage: 78.5,
        urban_cases: 456,
        rural_cases: 778,
        streaming_status: { has_buffer: false }
    };
    updateDashboardCards(mockData);
}

// Show error message
function showError() {
    document.getElementById('loading-cards').classList.add('hidden');
    document.getElementById('error-message').classList.remove('hidden');
}

// Handle date filter changes
function handleDateFilter() {
    const filterValue = document.getElementById('dateFilter').value;
    
    if (filterValue === 'advanced') {
        // Show advanced filter modal/form
        alert('Advanced filter coming soon!');
        document.getElementById('dateFilter').value = 'all';
        return;
    }
    
    // Show loading state
    document.getElementById('stats-cards').classList.add('hidden');
    document.getElementById('loading-cards').classList.remove('hidden');
    
    // Reload data with filter
    loadDashboardStats(filterValue);
}

// Update loadDashboardStats to accept date filter
async function loadDashboardStatsWithFilter(days = null) {
    try {
        let API_URL = 'https://sentinel-dashboard-api-196547645490.asia-south1.run.app/api/dashboard/stats';
        
        if (days && days !== 'all') {
            API_URL += `?days=${days}`;
        }
        
        console.log('Fetching data from:', API_URL);
        const response = await fetch(API_URL);
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const result = await response.json();
        console.log('API Response:', result);
        
        if (result.success && result.data) {
            updateDashboardCards(result.data);
        } else {
            console.error('API returned unsuccessful response:', result);
            showMockData();
        }
    } catch (error) {
        console.error('Error loading dashboard stats:', error);
        showMockData();
    }
}

// Wrapper function for backward compatibility
function loadDashboardStats(days = null) {
    loadDashboardStatsWithFilter(days);
}

