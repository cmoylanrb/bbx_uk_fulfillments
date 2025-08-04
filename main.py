import functions_framework
import logging
from flask import Request, Response

# Setup logging
logging.basicConfig(level=logging.INFO)

@functions_framework.http
def router(request: Request) -> Response:
    """Main router for BBX UK Fulfillments Cloud Functions."""
    try:
        # Get the function name from the request path
        function_name = request.path.strip('/') if request.path else ''
        
        # If no function specified in path, check JSON data
        if not function_name:
            try:
                import json
                data = request.get_json()
                if data and 'route' in data:
                    function_name = data['route']
                else:
                    function_name = 'fulfillments_exp'
            except:
                function_name = 'fulfillments_exp'
        
        logging.info(f"Routing to function: {function_name}")
        
        if function_name == 'fulfillments_exp':
            from bbx_uk_fulfillments_exp import process_fulfillments
            return process_fulfillments(request)
        elif function_name == 'shipments_imp':
            from bbx_uk_shipments_imp import process_shipments
            return process_shipments(request)
        elif function_name == 'stock_imp':
            from bbx_uk_stock_imp import process_stock
            return process_stock(request)
        else:
            return {
                'status': 'error',
                'message': f'Unknown function: {function_name}',
                'available_functions': ['fulfillments_exp', 'shipments_imp', 'stock_imp']
            }, 404
            
    except Exception as e:
        logging.error(f"Error in router: {str(e)}")
        return {
            'status': 'error',
            'message': f'Router error: {str(e)}'
        }, 500

@functions_framework.http
def fulfillments_exp(request: Request) -> Response:
    """Direct handler for fulfillment processing."""
    from bbx_uk_fulfillments_exp import process_fulfillments
    return process_fulfillments(request)

@functions_framework.http
def shipments_imp(request: Request) -> Response:
    """Direct handler for shipment processing."""
    from bbx_uk_shipments_imp import process_shipments
    return process_shipments(request)

@functions_framework.http
def stock_imp(request: Request) -> Response:
    """Direct handler for stock processing."""
    from bbx_uk_stock_imp import process_stock
    return process_stock(request) 