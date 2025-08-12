
from datetime import date, datetime
from bs4 import BeautifulSoup
import pandas as pd
from auxiliar import get_arquivo


def EnviarEmail(DfStatus, TB_GruposMudaram, TB_ResumoCurva):
    arquivo_csv = get_arquivo("GetHiperlink.csv")

    Df_CodeInfo = pd.read_csv(arquivo_csv, sep=";")

    DfStatus = DfStatus.set_index("Cod_HTTP").join(Df_CodeInfo.set_index("Code"),on="Cod_HTTP")
        

    arquivo_html = get_arquivo("Email_Semanal.html")

    #abrindo e lendo o arquivo HTML
    with open(arquivo_html, "r", encoding="utf-8") as file :
        HTML_EmailSemanal = file.read()

    #Transoformando o HTML de uma forma que o python consegue interagir
    soup = BeautifulSoup(HTML_EmailSemanal, "html.parser")

    tbody_inativos = soup.find(class_="Inativos")

    tbody_ativos = soup.find(class_="Ativos")

    tbody_curva = soup.find(class_="Curva")

    #TB_MudouStatus = TB_Final[TB_Final["MudouStatus?"]==1]

    #for index, row in TB_MudouStatus.iterrows():
    #    Nova_Linha = ""

    #    if row["Status"] == 172:
    #        Nova_Linha = f"""<tr>
    #                            <td>{row["Grupo Econômico"]}</td>
    #                         </tr>"""
    #        Nova_Linha = BeautifulSoup(Nova_Linha, "html.parser").tr
    #        tbody_inativos.append(Nova_Linha)
    #    else:
    #        Nova_Linha = f"""<tr>
    #                            <td>{row["Grupo Econômico"]}</td>
    #                         </tr>"""
    #        Nova_Linha = BeautifulSoup(Nova_Linha, "html.parser").tr
    #        tbody_ativos.append(Nova_Linha)




    mapa = {27: "A", 28: "B", 29: "C"}

    for index, row in TB_GruposMudaram.iterrows():
        Atual = mapa.get(row["Atual"], row["Atual"])
        Antigo = mapa.get(row["Antigo"], row["Antigo"])

        LinhasCurva = ""
        
        if row["Atual"] < row["Antigo"]:  
            LinhasCurva = f"""<tr>
                                <td>{index}</td>
                                <td>{Antigo}</td>
                                <td style="color: green">{Atual} ▲</td>
                            </tr>"""
            LinhasCurva = BeautifulSoup(LinhasCurva, "html.parser").tr
            tbody_curva.append(LinhasCurva)
        else:
            LinhasCurva = f"""<tr>
                                <td>{index}</td>
                                <td >{Antigo}</td>
                                <td style="color: red">{Atual} ▼</td>
                            </tr>"""
            LinhasCurva = BeautifulSoup(LinhasCurva, "html.parser").tr
            tbody_curva.append(LinhasCurva)

    # Salvando o HTML alterado em uma variável
    HTML_EmailSemanal = str(soup)


    arquivo_html = get_arquivo("Email_Diario.html")

    #abrindo e lendo o arquivo HTML
    with open(arquivo_html, "r", encoding="utf-8") as file :
        HTML_EmailDiario = file.read()

    #Transoformando o HTML de uma forma que o python consegue interagir
    soup = BeautifulSoup(HTML_EmailDiario, "html.parser")    

    tbody_QtdeGrupos = soup.find(class_="QtdeGruposEconomicos")
    tbody_curva = soup.find(class_="Curva")


    Linhas = ""
    for index, row in TB_ResumoCurva.iterrows():
        Linhas =f"""<tr>
                        <td>{index}</td>
                        <td>{row["Grupos"]}</td>
                        <td>{row["Empresas"]}</td>
                    </tr>"""
        Linhas = BeautifulSoup(Linhas, "html.parser").tr
        tbody_QtdeGrupos.append(Linhas)


    mapa = {27: "A", 28: "B", 29: "C"}

    for index, row in TB_GruposMudaram.iterrows():
        Atual = mapa.get(row["Atual"], row["Atual"])
        Antigo = mapa.get(row["Antigo"], row["Antigo"])

        LinhasCurva = ""
        
        if row["Atual"] < row["Antigo"]:  
            LinhasCurva = f"""<tr>
                                <td>{index}</td>
                                <td>{Antigo}</td>
                                <td style="color: green">{Atual} ▲</td>
                            </tr>"""
            LinhasCurva = BeautifulSoup(LinhasCurva, "html.parser").tr
            tbody_curva.append(LinhasCurva)
        else:
            LinhasCurva = f"""<tr>
                                <td>{index}</td>
                                <td >{Antigo}</td>
                                <td style="color: red">{Atual} ▼</td>
                            </tr>"""
            LinhasCurva = BeautifulSoup(LinhasCurva, "html.parser").tr
            tbody_curva.append(LinhasCurva)




    # Salvando o HTML alterado em uma variável
    HTML_EmailDiario = str(soup)        


    def EnviarEmailHTML(Titulo,HTML,Destinatarios,Anexo):

        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        from email.mime.base import MIMEBase
        from email import encoders
        import os

        # Configurações do e-mail
        smtp_server = "smtp.gmail.com"
        smtp_port = 587
        sender_email = "bi@metarh.com.br"  
        sender_password = "BI@2025_2TRI"         
        receiver_email = Destinatarios 

        #Data da atualização
        Hoje = datetime.now().strftime("%d_%m_%Y")

        h = Hoje.replace("_","/")
        # Criando a mensagem
        subject = Titulo + " - " + h


        # Configurando a mensagem MIME
        msg = MIMEMultipart()
        msg["From"] = sender_email
        #msg["To"] = receiver_email   Ajustando para todos em copia oculta
        msg["To"] = ""
        msg["Subject"] = subject


        if Anexo == True:
            file_path = f"Atualizacao/Atualizacao_{Hoje}.csv"


            DfStatus.to_csv(file_path)

            # Anexando o arquivo CSV com nome simples
            with open(file_path, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f"attachment; filename=Atualizacao_{Hoje}.csv")  # Nome simples do arquivo
                msg.attach(part)    


        msg.attach(MIMEText(HTML, "html"))

        # Enviando o e-mail
        try:
            # Conectando ao servidor SMTP
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.set_debuglevel(0)  # Habilita o modo de depuração (opcional)
            server.starttls()  # Inicia a criptografia TLS
            server.login(sender_email, sender_password)  # Faz o login

            # Envia o e-mail
            text = msg.as_string()  # Converte a mensagem para string
            server.sendmail(sender_email, receiver_email, text)  # Envia o e-mail
            return print("E-mail enviado com sucesso!")
        except Exception as e:
            return print(f"Erro ao enviar o e-mail: {e}")

        finally:
            server.quit()  # Encerra a conexão com o servidor

        

    Hoje = datetime.now()
    DiaSemana = date.weekday(Hoje)


    Semana = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]

    print(Semana[date.weekday(Hoje)])
    print( Hoje )

    DestinatariosSemanal = ["rebecaleite@metarh.com.br",
    "paulosilva@metarh.com.br",
    "kellyarnold@metarh.com.br",
    "leandromarques@metarh.com.br",
    "daniellemonteiro@metarh.com.br",
    "marciaishikura@metarh.com.br",
    "wilbersilva@metarh.com.br",
    "rejanesouza@metarh.com.br",
    "carolinegiorgetti@metarh.com.br",
    "mariaalice@metarh.com.br",
    "fernandabastos@metarh.com.br", 
    "gersonvieira@metarh.com.br", 
    "stefaniemedeiros@metarh.com.br"]

    DestinatariosDiarios = ["gersonvieira@metarh.com.br", "stefaniemedeiros@metarh.com.br"]
    """
    if DiaSemana == 4:
        EnviarEmailHTML("Atualização Automática Semanal", HTML_EmailSemanal, DestinatariosSemanal,False)
        EnviarEmailHTML("Atualização Automática Diária", HTML_EmailDiario, DestinatariosDiarios,True)
    else:
        EnviarEmailHTML("Atualização Automática Diária", HTML_EmailDiario, DestinatariosDiarios,True)    
    """