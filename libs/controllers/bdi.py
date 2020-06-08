from ..utils.constants import *
import datetime


class BDIController:
    def __init__(self, repository):
        self.__repository = repository

    def writeAction(self, sessionCode, typeService):
        try:
            payload = {
                "app": API_LOG_NAME_APP,
                "servicio": typeService,
                "codigopais": API_LOG_PREFIX_COUNTRY,
                "estadoEjecucion": "Pendiente",
                "tipoEjecucion": "Automática",
                "usuarioEjecucion": "etl-linkedtactics",
                "fechaInicio": datetime.datetime.now().isoformat(),
                "input": {"TiempoEspera": "2000"},
                "output": {}
            }

            isCallAPICorrect = self.__repository.callAPI(
                sessionCode, payload, "")

            return isCallAPICorrect
        except Exception as exception:
            print("An error ocurred in 'libs.controllers.bdi': writeAction")
            print(f"Error: {exception}")

    def writeState(self, sessionCode, event, message):
        try:
            payload = {
                "fechaEjecucion": datetime.datetime.now().isoformat(),
                "fechaInicio": datetime.datetime.now().isoformat(),
                "tipoEvento": "info",
                "evento": event,
                "duracion": 0,
                "mensaje": message
            }

            self.__repository.callAPI(sessionCode, payload, "add/")

            return True
        except Exception as exception:
            print("An error ocurred in 'libs.controllers.bdi': writeState")
            print(f"Error: {exception}")
            return False

    def writeStateMs(self, sessionCode, event, message, ms):
        try:
            payload = {
                "fechaEjecucion": datetime.datetime.now().isoformat(),
                "fechaInicio": datetime.datetime.now().isoformat(),
                "tipoEvento": "info",
                "evento": event,
                "duracion": round(ms, 2),
                "mensaje": message
            }

            self.__repository.callAPI(sessionCode, payload, "add/")

            return True
        except Exception as exception:
            print("An error ocurred in 'libs.controllers.bdi': writeStateMs")
            print(f"Error: {exception}")
            return False

    def writeFailed(self, sessionCode, message):
        try:
            payload = {
                "fechaFin": datetime.datetime.now().isoformat(),
                "mensaje": message
            }

            self.__repository.callAPI(sessionCode, payload, "error/")

            return True
        except Exception as exception:
            print("An error ocurred in 'libs.controllers.bdi': writeFailed")
            print(f"Error: {exception}")
            return False

    def writeDone(self, sessionCode):
        try:
            payload = {
                "fechaFin": datetime.datetime.now().isoformat()
            }

            self.__repository.callAPI(sessionCode, payload, "done/")

            return True
        except Exception as exception:
            print("An error ocurred in 'libs.controllers.bdi': writeDone")
            print(f"Error: {exception}")
            return False
