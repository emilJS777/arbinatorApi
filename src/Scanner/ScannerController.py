from src.Scanner.ScannerService import ScannerService
from src.__Parents.Controller import Controller


class ScannerDiagnosticsController(Controller):
    def get(self):
        return {"success": True, "obj": ScannerService.diagnostics_payload()}
