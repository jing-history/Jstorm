package wang.jinggo.web;

import wang.jinggo.domain.LatLngBean;
import wang.jinggo.service.StormService;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/**
 * @author wangyj
 * @description
 * @create 2018-09-02 16:55
 **/
@WebServlet(urlPatterns = "/index.do")
public class StormServlet extends HttpServlet {

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        doPost(request, response);
    }


    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html;charset=UTF-8");
        request.setCharacterEncoding("UTF-8");
        PrintWriter out = response.getWriter();
        String method = request.getParameter("method");
        list(request, response);
    }

    public void list(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        StormService sto = new StormService();
        List<LatLngBean> list = sto.findAll();

        request.setAttribute("StormList", list);
        request.getRequestDispatcher("index.jsp").forward(request, response);
    }
}
