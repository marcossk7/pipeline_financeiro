import polars as pl
import yfinance as yf
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)

def extract(ticket:str='USDBRL=X') -> pl.DataFrame:
    
    try:
        logger.info('Extraindo dados do dólar do yfinance')
        dolar_history = yf.download(
            tickers=ticket,
            period='max',
            interval='1mo'
        )
        return pl.from_pandas(dolar_history.reset_index())
    
    except Exception as err:
        err_type = type(err)
        logger.exception(f'err_type: {err_type}')
        logger.error(err)
        raise
