from fpdf import FPDF


def create_pdf(surveyId, title, text_map, filename="pdf"):
    pdf = FPDF()
    pdf.add_font("CenturyGothic", style="", fname="font/CenturyGothic.ttf")
    pdf.add_font("CenturyGothic", style="b", fname="font/CenturyGothic-Bold.ttf")
    pdf.add_font("CenturyGothic", style="i", fname="font/CenturyGothic-Italic.ttf")
    pdf.add_font("CenturyGothic", style="bi", fname="font/CenturyGothic-BoldItalic.ttf")

    pdf.add_page()
    pdf.set_font("CenturyGothic", "B", 22)
    pdf.multi_cell(
        0,
        text=f"{title} ({surveyId})",
        new_x="LEFT",
        new_y="NEXT",
        align="C",
        padding=5,
    )
    pdf.set_font("CenturyGothic", "", 14)
    pdf.multi_cell(
        0,
        text="Eine Zusammenfassung der Antworten auf die Fragen des Fragebogens.",
        new_x="LEFT",
        new_y="NEXT",
        align="C",
        padding=5,
    )
    pdf.set_font("CenturyGothic", "", 8)
    pdf.multi_cell(
        0,
        text="Dieses Dokument wurde automatisch erstellt.",
        new_x="LEFT",
        new_y="NEXT",
        align="C",
        padding=5,
    )

    for question in text_map.values():
        pdf.add_page()
        pdf.set_font("CenturyGothic", "b", 16)
        pdf.multi_cell(
            0,
            text=f"Frage: {question.get('question')} ({question.get('title')})",
            new_x="LEFT",
            new_y="NEXT",
            padding=(0, 0, 5, 0),
        )
        if question.get("help") is not None and question.get("help") != "":
            pdf.set_font("CenturyGothic", "", 13)
            pdf.multi_cell(
                0,
                text=f"Hilfetext: {question.get('help')}",
                new_x="LEFT",
                new_y="NEXT",
                padding=(0, 0, 10, 4),
            )

        pdf.set_fill_color(230, 230, 230)
        pdf.set_font("CenturyGothic", "B", 12)
        pdf.multi_cell(
            0,
            text="Antworten:",
            new_x="LEFT",
            new_y="NEXT",
            padding=(3, 0, 1, 5),
            fill=True,
        )
        pdf.set_font("CenturyGothic", "", 12)

        i = 0
        for answer in question.get("answers"):
            i += 1
            answer_text = f"{i}. {answer}"
            pdf.multi_cell(
                0,
                text=answer_text,
                new_x="LEFT",
                new_y="NEXT",
                padding=(1, 0, 2, 10),
                fill=True,
            )

    pdf.output(filename + ".pdf")


# if __name__ == "__main__":
#     data = {
#         "G01Q01": {
#             "answers": ["a", "test", "asdf", "eafdqwe123132"],
#             "title": "G01Q01",
#             "question": "Name",
#             "help": "Wir benötigen deinen Namen, um dich zu registrieren.",
#         },
#         "G01Q02": {
#             "answers": ["test", "asdf", "1"],
#             "title": "G01Q02",
#             "question": "JG/Ort",
#             "help": "",
#         },
#     }
#     create_pdf("123456", "Test", data)
