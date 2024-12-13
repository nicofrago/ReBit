# ReBit

This project analyses the impact of social media sentiment on Bitcoin price. Reddit is used as 
the primary data source. The sentiment analysis is performed using the VADER algorithm. 
Then, the results are explored to identify their correlation with Bitcoin price fluctuations.

The project provides a publicly accessible dashboard with real-time 
sentiment information about Bitcoin and sends a daily WhatsApp message with the summary 
of the day decision-makers, offering actionable insights to support their investment decisions.

## Project Structure

The project consists of the following files and directories:

- `dashboard/`
    - `app.py`: Contains the main function to run the dashboard application.
    - `utils.py`: Utility functions used across the dashboard application.
- `BUCKET/`
    - `bitcoin_data`: Collected Bitcoin price data for a one-week period.
    - `bitcoin_reddit_comments.csv`: Collected Reddit comments about Bitcoin for a one-week.
- `README.md`: This file, providing an overview of the project.
- `requirements.txt`: Lists the Python dependencies required for the project.

## Getting Started

To get started with the project, follow these steps:

1. Clone the repository:
     ```sh
     git clone https://github.com/yourusername/rebit.git
     ```
2. Navigate to the project directory:
     ```sh
     cd rebit
     ```
3. Install the required dependencies:
     ```sh
     pip install -r requirements.txt
     ```
4. Run the dashboard application:
     ```sh
     python dashboard/app.py
     ```
     
## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.